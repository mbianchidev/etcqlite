package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	historyBatchSize      = 512
	historyFormat         = 2
	maxHistoryFileBytes   = 16 << 30
	maxHistoryRecordBytes = 8 << 20
	maxWatchInterruptions = 64
)

type watchKV struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	Revision       int64  `json:"revision"`
	CreateRevision int64  `json:"create_revision"`
	Version        int64  `json:"version"`
	Lease          int64  `json:"lease"`
}

type watchEntry struct {
	watchKV
	Deleted bool     `json:"deleted"`
	PrevKV  *watchKV `json:"prev_kv,omitempty"`
}

type watchInterruption struct {
	Code  string `json:"code"`
	Error string `json:"error"`
}

type historyRecord struct {
	Kind          string              `json:"kind"`
	Format        int                 `json:"format"`
	Engine        string              `json:"engine,omitempty"`
	Seed          int64               `json:"seed,omitempty"`
	Versions      []string            `json:"server_versions,omitempty"`
	Prefix        string              `json:"prefix,omitempty"`
	Epoch         int                 `json:"epoch,omitempty"`
	Operations    []historyOperation  `json:"operations,omitempty"`
	Watch         []watchEntry        `json:"watch,omitempty"`
	StartRevision int64               `json:"start_revision,omitempty"`
	EndRevision   int64               `json:"end_revision,omitempty"`
	FinalKV       []watchKV           `json:"final_kv,omitempty"`
	Reconnects    int                 `json:"watch_reconnects,omitempty"`
	Interruptions []watchInterruption `json:"watch_interruptions,omitempty"`
	DurationNS    int64               `json:"requested_duration_ns,omitempty"`
	ElapsedNS     int64               `json:"elapsed_ns,omitempty"`
	Failure       string              `json:"failure,omitempty"`
	Check         string              `json:"check,omitempty"`
}

func checkHistoryFile(path string) (result error) {
	before, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() || before.Mode().Perm()&0o077 != 0 || before.Size() > maxHistoryFileBytes {
		return errors.New("history must be a private regular file of at most 16 GiB")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(before, info) {
		return errors.New("history changed while opening")
	}
	scanner := bufio.NewScanner(io.LimitReader(file, maxHistoryFileBytes+1))
	scanner.Buffer(make([]byte, 64<<10), maxHistoryRecordBytes)
	epochs := 0
	state := "manifest"
	var base string
	var duration, lastReturn int64
	for scanner.Scan() {
		line := scanner.Bytes()
		if err := uniqueJSONFields(line); err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		var record historyRecord
		if err := decoder.Decode(&record); err != nil {
			return err
		}
		if err := historyRecordShape(line, record.Kind); err != nil {
			return err
		}
		if record.Format != historyFormat {
			return errors.New("unsupported external history format")
		}
		if state == "complete" || record.Failure != "" {
			return errors.New("history has trailing records or captured failure")
		}
		if record.Kind != "epoch" && (len(record.Operations) != 0 || len(record.Watch) != 0 ||
			record.StartRevision != 0 || record.EndRevision != 0 || len(record.FinalKV) != 0 ||
			record.Reconnects != 0 || len(record.Interruptions) != 0) {
			return errors.New("history evidence appears outside its epoch")
		}
		if record.Kind != "manifest" && (record.Engine != "" || record.Seed != 0 || len(record.Versions) != 0) {
			return errors.New("history identity appears outside its manifest")
		}
		if (record.Kind != "manifest" && record.DurationNS != 0) || (record.Kind != "complete" && record.ElapsedNS != 0) {
			return errors.New("history timing appears outside its manifest or completion")
		}
		switch record.Kind {
		case "manifest":
			if state != "manifest" || record.Engine == "" || len(record.Versions) != 3 || record.Epoch != 0 || record.Check != "" ||
				record.Prefix != fmt.Sprintf("mock/robustness/%d/", record.Seed) || record.DurationNS <= 0 || record.DurationNS > int64(24*time.Hour) {
				return errors.New("history must start with one valid fixture manifest")
			}
			for _, version := range record.Versions {
				if version == "" {
					return errors.New("history manifest has an empty server version")
				}
			}
			base, duration, state = record.Prefix, record.DurationNS, "epoch"
		case "check":
			if state != "check" || record.Epoch != epochs-1 || record.Check != "Ok" || record.Prefix != "" {
				return errors.New("history needs exactly one matching successful check per epoch")
			}
			state = "epoch"
		case "complete":
			if state != "epoch" || epochs == 0 || record.Epoch != epochs || record.Check != "Ok" || record.Prefix != "" ||
				record.ElapsedNS < duration || record.ElapsedNS < lastReturn {
				return errors.New("history completion does not cover every checked epoch")
			}
			state = "complete"
		case "epoch":
			if state != "epoch" || record.Epoch != epochs || record.Prefix != fmt.Sprintf("%s%08d/", base, epochs) || record.Check != "" {
				return errors.New("history epochs are missing, repeated or out of order")
			}
			if _, err := checkHistory(record.Operations, 30*time.Second); err != nil {
				return err
			}
			var end int64
			for _, operation := range record.Operations {
				if operation.Call < lastReturn {
					return errors.New("history epoch overlaps an earlier completed collection")
				}
				end = max(end, operation.Return)
			}
			lastReturn = end
			if err := validateWatch(record); err != nil {
				return err
			}
			epochs++
			state = "check"
		default:
			return errors.New("unsupported history record kind")
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("bounded history record: %w", err)
	}
	if state != "complete" {
		return errors.New("history is incomplete or has no checked epochs")
	}
	after, err := file.Stat()
	if err != nil {
		return err
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !os.SameFile(info, pathInfo) || after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) {
		return errors.New("history changed during checking")
	}
	return nil
}

func uniqueJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var value func(int) error
	value = func(depth int) error {
		if depth > 16 {
			return errors.New("history JSON exceeds its nesting bound")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if token == nil {
			return errors.New("history JSON does not allow null fields")
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
			return nil
		}
		keys := make(map[string]bool)
		for decoder.More() {
			if delimiter == '{' {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || keys[name] {
					return errors.New("history JSON has a duplicate or invalid field")
				}
				keys[name] = true
			}
			if err := value(depth + 1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("history record has trailing JSON")
	}
	return nil
}

func historyFields(data json.RawMessage, required ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for _, name := range required {
		if _, exists := fields[name]; !exists {
			return nil, fmt.Errorf("history JSON is missing required field %q", name)
		}
	}
	return fields, nil
}

func historyRecordShape(data []byte, kind string) error {
	required := []string{"kind", "format"}
	switch kind {
	case "manifest":
		required = append(required, "engine", "server_versions", "prefix", "requested_duration_ns")
	case "epoch":
		required = append(required, "prefix", "operations", "start_revision", "end_revision")
	case "check":
		required = append(required, "check")
	case "complete":
		required = append(required, "epoch", "check", "elapsed_ns")
	}
	fields, err := historyFields(data, required...)
	if err != nil || kind != "epoch" {
		return err
	}
	var operations []json.RawMessage
	if err := json.Unmarshal(fields["operations"], &operations); err != nil {
		return err
	}
	for _, raw := range operations {
		operation, err := historyFields(raw, "client", "call_ns", "return_ns", "input", "output", "revision")
		if err != nil {
			return err
		}
		if _, err := historyFields(operation["input"], "group", "kind"); err != nil {
			return err
		}
		output, err := historyFields(operation["output"], "pair")
		if err != nil {
			return err
		}
		var pair []string
		if err := json.Unmarshal(output["pair"], &pair); err != nil {
			return err
		}
		if len(pair) != 2 {
			return errors.New("history transaction pair must contain exactly two strings")
		}
	}
	kvFields := []string{"key", "value", "revision", "create_revision", "version", "lease"}
	for _, name := range []string{"watch", "final_kv"} {
		raw, exists := fields[name]
		if !exists {
			continue
		}
		var entries []json.RawMessage
		if err := json.Unmarshal(raw, &entries); err != nil {
			return err
		}
		for _, entry := range entries {
			required := kvFields
			if name == "watch" {
				required = append(slices.Clone(kvFields), "deleted")
			}
			kv, err := historyFields(entry, required...)
			if err != nil {
				return err
			}
			if previous, exists := kv["prev_kv"]; exists {
				if _, err := historyFields(previous, kvFields...); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func writeHistoryRecord(file *os.File, record historyRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if len(data)+1 >= maxHistoryRecordBytes || info.Size()+int64(len(data))+1 > maxHistoryFileBytes {
		return errors.New("history evidence exceeds its file or record bound")
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func runCorrectness(o options) (result error) {
	if !o.owned || o.output == "" || o.engine == "" || o.concurrency < 1 || o.concurrency > 32 || o.duration <= 0 || o.duration > 24*time.Hour {
		return errors.New("correctness collection needs owned fixtures, a new output, exact build identity, at most 32 workers and a bounded duration")
	}
	file, err := os.OpenFile(o.output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, writeHistoryRecord(file, historyRecord{Kind: "failure", Format: historyFormat, Failure: result.Error()}))
		}
		result = errors.Join(result, file.Sync(), file.Close())
	}()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	client, err := fixtureClient(o)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, client.Close()) }()
	versions := make([]string, 0, 3)
	for _, endpoint := range strings.Split(o.endpoints, ",") {
		call, done := context.WithTimeout(ctx, 10*time.Second)
		response, err := client.Status(call, endpoint)
		done()
		if err != nil {
			return err
		}
		versions = append(versions, response.Version)
	}
	base := fmt.Sprintf("mock/robustness/%d/", o.seed)
	call, done := context.WithTimeout(ctx, 10*time.Second)
	empty, err := client.Get(call, base, clientv3.WithPrefix(), clientv3.WithCountOnly())
	done()
	if err != nil {
		return err
	}
	if empty.Count != 0 {
		return errors.New("correctness namespace already exists; never delete or reuse it")
	}
	if err := writeHistoryRecord(file, historyRecord{Kind: "manifest", Format: historyFormat, Engine: o.engine, Seed: o.seed, Versions: versions, Prefix: base, DurationNS: int64(o.duration)}); err != nil {
		return err
	}
	start := time.Now()
	epochs := 0
	var operationCount, uncertainCount int
	for ; time.Since(start) < o.duration && ctx.Err() == nil; epochs++ {
		epoch := epochs
		prefix := fmt.Sprintf("%s%08d/", base, epoch)
		record, err := collectHistory(ctx, client, prefix, o, epoch, start)
		operationCount += len(record.Operations)
		for _, operation := range record.Operations {
			if operation.Output.Uncertain {
				uncertainCount++
			}
		}
		if err != nil {
			record.Failure = err.Error()
		}
		if writeErr := writeHistoryRecord(file, record); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		if err != nil {
			return err
		}
		checked, err := checkHistory(record.Operations, 30*time.Second)
		if err == nil {
			err = validateWatch(record)
		}
		check := historyRecord{Kind: "check", Format: historyFormat, Epoch: epoch, Check: string(checked)}
		if err != nil {
			check.Failure = err.Error()
		}
		if writeErr := writeHistoryRecord(file, check); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		if err != nil {
			return err
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if epochs == 0 {
		return errors.New("correctness collection completed no epochs")
	}
	elapsed := time.Since(start)
	if err := writeHistoryRecord(file, historyRecord{Kind: "complete", Format: historyFormat, Epoch: epochs, ElapsedNS: elapsed.Nanoseconds(), Check: "Ok"}); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"event": "finite_history_checked", "epochs": epochs, "operations": operationCount,
		"uncertain_operations": uncertainCount, "elapsed_seconds": elapsed.Seconds(), "production_claim": false,
	})
}

func collectHistory(ctx context.Context, client *clientv3.Client, prefix string, o options, epoch int, start time.Time) (historyRecord, error) {
	record := historyRecord{Kind: "epoch", Format: historyFormat, Prefix: prefix, Epoch: epoch}
	call, done := context.WithTimeout(ctx, 10*time.Second)
	begin, err := client.Get(call, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	done()
	if err != nil {
		return record, err
	}
	if begin.Header == nil || begin.Count != 0 {
		return record, errors.New("history epoch is not fresh or has no revision header")
	}
	record.StartRevision = begin.Header.Revision
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	var mu sync.Mutex
	var operations []historyOperation
	var events []watchEntry
	var failure error
	noteInterruption := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if len(record.Interruptions) >= maxWatchInterruptions {
			failure = errors.Join(failure, errors.New("watch interruption evidence exceeds its epoch bound"))
			stopWatch()
			return
		}
		record.Interruptions = append(record.Interruptions, watchInterruption{Code: transientCode(err).String(), Error: err.Error()})
	}
	progress := make(chan int64, 1)
	var watchers sync.WaitGroup
	watchers.Add(1)
	go func() {
		defer watchers.Done()
		next := begin.Header.Revision + 1
		for watchCtx.Err() == nil {
			one, cancel := context.WithCancel(watchCtx)
			stream := client.Watch(one, prefix, clientv3.WithPrefix(), clientv3.WithRev(next), clientv3.WithPrevKV(), clientv3.WithProgressNotify(), clientv3.WithCreatedNotify())
			count := 0
			reconnect := false
			for response := range stream {
				if response.Err() != nil {
					if watchCtx.Err() == nil {
						if transientCode(response.Err()) != codes.OK {
							noteInterruption(response.Err())
							reconnect = true
							break
						}
						mu.Lock()
						failure = errors.Join(failure, response.Err())
						mu.Unlock()
					}
					cancel()
					return
				}
				if response.Header == nil {
					mu.Lock()
					failure = errors.Join(failure, errors.New("watch response has no revision header"))
					mu.Unlock()
					cancel()
					return
				}
				mu.Lock()
				for _, event := range response.Events {
					if event == nil || event.Kv == nil || (event.Type != clientv3.EventTypePut && event.Type != clientv3.EventTypeDelete) {
						failure = errors.Join(failure, errors.New("watch returned an invalid event shape"))
						stopWatch()
						break
					}
					if len(events) >= historyBatchSize*8 {
						failure = errors.Join(failure, errors.New("watch evidence exceeds bounded epoch capacity"))
						stopWatch()
						break
					}
					entry := watchEntry{watchKV: capturedKV(event.Kv), Deleted: event.Type == clientv3.EventTypeDelete}
					if event.PrevKv != nil {
						previous := capturedKV(event.PrevKv)
						entry.PrevKV = &previous
					}
					events = append(events, entry)
					next = event.Kv.ModRevision + 1
					count++
				}
				mu.Unlock()
				if response.IsProgressNotify() {
					select {
					case progress <- response.Header.Revision:
					default:
					}
				}
				if count >= 128 {
					reconnect = true
					mu.Lock()
					record.Reconnects++
					mu.Unlock()
					break
				}
			}
			cancel()
			if watchCtx.Err() == nil && !reconnect {
				mu.Lock()
				failure = errors.Join(failure, errors.New("watch stream ended without cancellation or a recorded interruption"))
				mu.Unlock()
				return
			}
			if reconnect {
				select {
				case <-watchCtx.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}()
	var workers sync.WaitGroup
	kinds := []string{"write", "read", "cas", "txn-write", "txn-read", "append", "list-read", "set-add", "set-remove", "set-read"}
	for worker := 0; worker < o.concurrency; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			random := rand.New(rand.NewSource(o.seed + int64(epoch)*10_000 + int64(worker)))
			last := ""
			for index := worker; index < historyBatchSize && ctx.Err() == nil; index += o.concurrency {
				input := historyInput{Group: random.Intn(16), Kind: kinds[index%len(kinds)], Value: fmt.Sprintf("mock-%d-%d", epoch, index), Expected: last}
				if input.Kind == "set-add" || input.Kind == "set-remove" {
					input.Value = fmt.Sprintf("mock-set-%d", (index/10)%4)
				}
				call, done := context.WithTimeout(ctx, 5*time.Second)
				invoked := time.Since(start).Nanoseconds()
				output, revision, err := historyRequest(call, client, prefix, input)
				completed := time.Since(start).Nanoseconds()
				done()
				mu.Lock()
				record := historyOperation{Client: worker, Call: invoked, Return: completed, Input: input, Output: output, Revision: revision}
				if err != nil {
					record.Error = err.Error()
					if transientCode(err) != codes.OK {
						record.Output.Uncertain = true
					} else {
						failure = errors.Join(failure, err)
					}
				}
				operations = append(operations, record)
				mu.Unlock()
				if err == nil && input.Kind == "read" {
					last = output.Value
				}
			}
		}(worker)
	}
	workers.Wait()
	call, done = context.WithTimeout(ctx, 10*time.Second)
	finish, err := client.Get(call, prefix, clientv3.WithPrefix())
	done()
	if err != nil {
		mu.Lock()
		failure = errors.Join(failure, err)
		mu.Unlock()
	} else if finish.Header == nil {
		mu.Lock()
		failure = errors.Join(failure, errors.New("fenced range has no revision header"))
		mu.Unlock()
	} else {
		record.EndRevision = finish.Header.Revision
		for _, kv := range finish.Kvs {
			if kv == nil {
				mu.Lock()
				failure = errors.Join(failure, errors.New("fenced range has an invalid KV"))
				mu.Unlock()
				break
			}
			record.FinalKV = append(record.FinalKV, capturedKV(kv))
		}
		deadline := time.NewTimer(10 * time.Second)
		tick := time.NewTicker(100 * time.Millisecond)
		complete := false
		for !complete {
			select {
			case revision := <-progress:
				complete = revision >= finish.Header.Revision
			case <-tick.C:
				call, done := context.WithTimeout(ctx, time.Second)
				err := client.RequestProgress(call)
				done()
				if err != nil && ctx.Err() == nil {
					if transientCode(err) != codes.OK {
						noteInterruption(err)
					} else {
						mu.Lock()
						failure = errors.Join(failure, err)
						mu.Unlock()
					}
				}
			case <-deadline.C:
				mu.Lock()
				failure = errors.Join(failure, errors.New("watch did not catch up to the fenced epoch revision"))
				mu.Unlock()
				complete = true
			case <-ctx.Done():
				mu.Lock()
				failure = errors.Join(failure, ctx.Err())
				mu.Unlock()
				complete = true
			}
		}
		tick.Stop()
		deadline.Stop()
	}
	stopWatch()
	watchers.Wait()
	record.Operations, record.Watch = operations, events
	return record, failure
}

func capturedKV(kv *mvccpb.KeyValue) watchKV {
	return watchKV{Key: string(kv.Key), Value: string(kv.Value), Revision: kv.ModRevision, CreateRevision: kv.CreateRevision, Version: kv.Version, Lease: kv.Lease}
}

func transientCode(err error) codes.Code {
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded
	}
	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}
	code := status.Code(err)
	if code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Canceled {
		return code
	}
	return codes.OK
}

func historyRequest(ctx context.Context, client clientv3.KV, prefix string, input historyInput) (historyOutput, int64, error) {
	group := prefix + strconv.Itoa(input.Group) + "/"
	getValue := func(key string) (string, int64, error) {
		response, err := client.Get(ctx, key)
		if err != nil {
			return "", 0, err
		}
		if response.Header == nil || response.Header.Revision <= 0 || len(response.Kvs) > 1 || (len(response.Kvs) == 1 && response.Kvs[0] == nil) {
			return "", 0, errors.New("invalid single-key range response")
		}
		if len(response.Kvs) == 0 {
			return "", response.Header.Revision, nil
		}
		return string(response.Kvs[0].Value), response.Header.Revision, nil
	}
	switch input.Kind {
	case "write", "set-add":
		key, value := group+"register", input.Value
		if input.Kind == "set-add" {
			key, value = group+"set/"+input.Value, "1"
		}
		response, err := client.Put(ctx, key, value)
		if err != nil {
			return historyOutput{}, 0, err
		}
		return historyOutput{}, response.Header.Revision, nil
	case "read", "list-read":
		key := group + "register"
		if input.Kind == "list-read" {
			key = group + "list"
		}
		value, revision, err := getValue(key)
		return historyOutput{Value: value}, revision, err
	case "cas":
		response, err := client.Txn(ctx).If(clientv3.Compare(clientv3.Value(group+"register"), "=", input.Expected)).Then(clientv3.OpPut(group+"register", input.Value)).Commit()
		if err != nil {
			return historyOutput{}, 0, err
		}
		return historyOutput{Succeeded: response.Succeeded}, response.Header.Revision, nil
	case "txn-write", "txn-read":
		ops := []clientv3.Op{clientv3.OpPut(group+"x", input.Value), clientv3.OpPut(group+"y", input.Value)}
		if input.Kind == "txn-read" {
			ops = []clientv3.Op{clientv3.OpGet(group + "x"), clientv3.OpGet(group + "y")}
		}
		response, err := client.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			return historyOutput{}, 0, err
		}
		output := historyOutput{}
		if input.Kind == "txn-read" {
			if len(response.Responses) != 2 {
				return output, 0, errors.New("transactional read response shape differs")
			}
			for index, record := range response.Responses {
				kv := record.GetResponseRange()
				if kv == nil || len(kv.Kvs) > 1 {
					return output, 0, errors.New("invalid transactional range shape")
				}
				if len(kv.Kvs) == 1 {
					output.Pair[index] = string(kv.Kvs[0].Value)
				}
			}
		}
		return output, response.Header.Revision, nil
	case "append":
		for ctx.Err() == nil {
			previous, _, err := getValue(group + "list")
			if err != nil {
				return historyOutput{}, 0, err
			}
			var values []string
			if previous != "" {
				if err := json.Unmarshal([]byte(previous), &values); err != nil {
					return historyOutput{}, 0, err
				}
			}
			if slices.Contains(values, input.Value) {
				return historyOutput{}, 0, errors.New("append invocation token already exists; ambiguous retries cannot be acknowledged as a new mutation")
			}
			encoded, err := json.Marshal(append(values, input.Value))
			if err != nil {
				return historyOutput{}, 0, err
			}
			comparison := clientv3.Compare(clientv3.Value(group+"list"), "=", previous)
			if previous == "" {
				comparison = clientv3.Compare(clientv3.Version(group+"list"), "=", 0)
			}
			response, err := client.Txn(ctx).If(comparison).Then(clientv3.OpPut(group+"list", string(encoded))).Commit()
			if err != nil {
				return historyOutput{}, 0, err
			}
			if response.Succeeded {
				return historyOutput{Value: string(encoded)}, response.Header.Revision, nil
			}
		}
		return historyOutput{}, 0, ctx.Err()
	case "set-remove":
		response, err := client.Delete(ctx, group+"set/"+input.Value)
		if err != nil {
			return historyOutput{}, 0, err
		}
		return historyOutput{Succeeded: response.Deleted != 0}, response.Header.Revision, nil
	case "set-read":
		response, err := client.Get(ctx, group+"set/", clientv3.WithPrefix())
		if err != nil {
			return historyOutput{}, 0, err
		}
		values := make([]string, 0, len(response.Kvs))
		for _, kv := range response.Kvs {
			if string(kv.Value) != "1" {
				return historyOutput{}, 0, errors.New("set membership value is invalid")
			}
			values = append(values, strings.TrimPrefix(string(kv.Key), group+"set/"))
		}
		slices.Sort(values)
		output := historyOutput{}
		if len(values) > 0 {
			encoded, err := json.Marshal(values)
			if err != nil {
				return output, 0, err
			}
			output.Value = string(encoded)
		}
		return output, response.Header.Revision, nil
	default:
		return historyOutput{}, 0, errors.New("unsupported synthetic history operation")
	}
}

func validateWatch(record historyRecord) error {
	if record.StartRevision <= 0 || record.EndRevision < record.StartRevision ||
		len(record.Watch) > historyBatchSize*8 || len(record.Interruptions) > maxWatchInterruptions ||
		record.Reconnects < 0 || record.Reconnects > len(record.Watch)/128 {
		return errors.New("watch evidence has invalid bounds")
	}
	for _, interruption := range record.Interruptions {
		if interruption.Error == "" || (interruption.Code != codes.Unavailable.String() &&
			interruption.Code != codes.DeadlineExceeded.String() && interruption.Code != codes.Canceled.String()) {
			return errors.New("watch evidence contains an unexplained interruption")
		}
	}
	current := make(map[string]watchKV)
	var fenced map[string]watchKV
	revisions := make(map[int64][]watchEntry)
	var last int64
	for _, event := range record.Watch {
		if event.Revision <= record.StartRevision || event.Revision < last ||
			!strings.HasPrefix(event.Key, record.Prefix) || event.Lease != 0 {
			return errors.New("watch event has an invalid revision, order or fixture key")
		}
		if event.Revision > record.EndRevision && fenced == nil {
			fenced = maps.Clone(current)
		}
		last = event.Revision
		for _, prior := range revisions[event.Revision] {
			if prior.Key == event.Key {
				return errors.New("watch reconnect duplicated an event")
			}
		}
		previous, exists := current[event.Key]
		if (event.PrevKV != nil) != exists || (exists && *event.PrevKV != previous) {
			return errors.New("watch previous KV is missing or altered")
		}
		if event.Deleted {
			if !exists || event.Value != "" || event.CreateRevision != 0 || event.Version != 0 {
				return errors.New("watch deletion has invalid metadata")
			}
			delete(current, event.Key)
		} else {
			creation, version := event.Revision, int64(1)
			if exists {
				creation, version = previous.CreateRevision, previous.Version+1
			}
			if event.CreateRevision != creation || event.Version != version {
				return errors.New("watch generation or version is invalid")
			}
			current[event.Key] = event.watchKV
		}
		revisions[event.Revision] = append(revisions[event.Revision], event)
	}
	if fenced == nil {
		fenced = current
	}
	acknowledged := make(map[int64]bool)
	var queries []historyOperation
	for _, operation := range record.Operations {
		if operation.Output.Uncertain || operation.Error != "" {
			continue
		}
		if operation.Revision < record.StartRevision || operation.Revision > record.EndRevision {
			return errors.New("acknowledged operation lies outside the fenced revisions")
		}
		queries = append(queries, operation)
		kind := operation.Input.Kind
		if kind == "read" || kind == "txn-read" || kind == "list-read" || kind == "set-read" ||
			((kind == "cas" || kind == "set-remove") && !operation.Output.Succeeded) {
			continue
		}
		if acknowledged[operation.Revision] || !watchMutationMatches(record.Prefix, operation, revisions[operation.Revision]) {
			return errors.New("acknowledged mutation is missing, duplicated or altered in watch history")
		}
		acknowledged[operation.Revision] = true
		delete(revisions, operation.Revision)
	}
	slices.SortFunc(queries, func(left, right historyOperation) int {
		if left.Revision < right.Revision {
			return -1
		}
		if left.Revision > right.Revision {
			return 1
		}
		return 0
	})
	timeline := make(map[string]watchKV)
	cursor := 0
	for _, operation := range queries {
		for cursor < len(record.Watch) && record.Watch[cursor].Revision <= operation.Revision {
			event := record.Watch[cursor]
			if event.Deleted {
				delete(timeline, event.Key)
			} else {
				timeline[event.Key] = event.watchKV
			}
			cursor++
		}
		if !watchReadMatches(record.Prefix, operation, timeline) {
			return errors.New("acknowledged read or no-op outcome differs from its captured revision")
		}
	}
	pendingUsed := make(map[int]bool)
	order := make([]int64, 0, len(revisions))
	for revision := range revisions {
		order = append(order, revision)
	}
	slices.Sort(order)
	for _, revision := range order {
		explained := false
		for index, operation := range record.Operations {
			if operation.Output.Uncertain && !pendingUsed[index] && watchMutationMatches(record.Prefix, operation, revisions[revision]) {
				pendingUsed[index], explained = true, true
				break
			}
		}
		if !explained {
			return errors.New("watch contains a mutation not explained by an acknowledged or uncertain invocation")
		}
	}
	if len(fenced) != len(record.FinalKV) {
		return errors.New("watch replay differs from the fenced final range")
	}
	for index, kv := range record.FinalKV {
		if index > 0 && record.FinalKV[index-1].Key >= kv.Key {
			return errors.New("fenced final range is not strictly key ordered")
		}
		if observed, exists := fenced[kv.Key]; !exists || observed != kv {
			return errors.New("watch replay has missing or altered final KV metadata")
		}
	}
	return nil
}

func watchReadMatches(prefix string, operation historyOperation, current map[string]watchKV) bool {
	group := prefix + strconv.Itoa(operation.Input.Group) + "/"
	switch operation.Input.Kind {
	case "read":
		return operation.Output.Value == current[group+"register"].Value
	case "txn-read":
		return operation.Output.Pair == [2]string{current[group+"x"].Value, current[group+"y"].Value}
	case "list-read":
		return operation.Output.Value == current[group+"list"].Value
	case "set-read":
		var values []string
		for key := range current {
			if strings.HasPrefix(key, group+"set/") {
				values = append(values, strings.TrimPrefix(key, group+"set/"))
			}
		}
		if len(values) == 0 {
			return operation.Output.Value == ""
		}
		slices.Sort(values)
		encoded, err := json.Marshal(values)
		return err == nil && operation.Output.Value == string(encoded)
	case "cas":
		if !operation.Output.Succeeded {
			value, exists := current[group+"register"]
			return !exists || value.Value != operation.Input.Expected
		}
	case "set-remove":
		if !operation.Output.Succeeded {
			_, exists := current[group+"set/"+operation.Input.Value]
			return !exists
		}
	}
	return true
}

func watchMutationMatches(prefix string, operation historyOperation, events []watchEntry) bool {
	group := prefix + strconv.Itoa(operation.Input.Group) + "/"
	expected := map[string]string{}
	deleted := false
	switch operation.Input.Kind {
	case "write", "cas":
		expected[group+"register"] = operation.Input.Value
		if operation.Input.Kind == "cas" && (len(events) != 1 || events[0].PrevKV == nil || events[0].PrevKV.Value != operation.Input.Expected) {
			return false
		}
	case "txn-write":
		expected[group+"x"], expected[group+"y"] = operation.Input.Value, operation.Input.Value
	case "append":
		if len(events) != 1 {
			return false
		}
		var values []string
		if events[0].PrevKV != nil && json.Unmarshal([]byte(events[0].PrevKV.Value), &values) != nil {
			return false
		}
		if slices.Contains(values, operation.Input.Value) {
			return false
		}
		encoded, err := json.Marshal(append(values, operation.Input.Value))
		if err != nil || (!operation.Output.Uncertain && operation.Output.Value != string(encoded)) {
			return false
		}
		expected[group+"list"] = string(encoded)
	case "set-add", "set-remove":
		expected[group+"set/"+operation.Input.Value] = "1"
		deleted = operation.Input.Kind == "set-remove"
	default:
		return false
	}
	if len(events) != len(expected) {
		return false
	}
	for _, event := range events {
		value, exists := expected[event.Key]
		if !exists || event.Deleted != deleted || (!deleted && event.Value != value) {
			return false
		}
	}
	return len(events) != 0
}
