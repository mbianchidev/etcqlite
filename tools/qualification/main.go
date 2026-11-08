package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const namespace = "mock/qualification/"
const maxLatencySamples = 100_000_000

type options struct {
	endpoints, ca, cert, key, output, engine string
	keys, mutations, concurrency             int
	warmup, duration                         time.Duration
	seed                                     int64
	owned                                    bool
	correctness                              bool
	checkHistory                             string
}

type latency struct {
	Count  int     `json:"count"`
	P50    float64 `json:"p50_us"`
	P95    float64 `json:"p95_us"`
	P99    float64 `json:"p99_us"`
	P999Us float64 `json:"p999_us"`
}

type report struct {
	Format          int                `json:"format"`
	Engine          string             `json:"engine"`
	Seed            int64              `json:"seed"`
	Keys            int                `json:"keys"`
	Mutations       int                `json:"retained_mutations"`
	KeyBytes        int                `json:"key_bytes"`
	ValueBytes      int                `json:"value_bytes"`
	Concurrency     int                `json:"concurrency"`
	Watches         int                `json:"watches"`
	WarmupSeconds   float64            `json:"warmup_seconds"`
	MeasuredSeconds float64            `json:"measured_seconds"`
	Throughput      float64            `json:"throughput"`
	Errors          int64              `json:"errors"`
	WarmupErrors    int64              `json:"warmup_errors"`
	WatchErrors     int64              `json:"watch_errors"`
	ErrorDetails    []requestError     `json:"error_details"`
	WarmupDetails   []requestError     `json:"warmup_error_details"`
	WatchDetails    []requestError     `json:"watch_error_details"`
	PerOperation    map[string]latency `json:"per_operation"`
	ServerVersions  []string           `json:"server_versions"`
	ClientOS        string             `json:"client_os"`
	ClientArch      string             `json:"client_arch"`
	ClientMaxProcs  int                `json:"client_max_procs"`
	PrimaryShape    bool               `json:"primary_workload_shape"`
	ProductionClaim bool               `json:"production_claim"`
	WatchShape      string             `json:"watch_shape"`
	Consistency     string             `json:"consistency"`
	SampleLimit     int                `json:"latency_sample_limit"`
	Phase           string             `json:"phase"`
	Completed       bool               `json:"completed"`
	Failure         string             `json:"failure,omitempty"`
	SeededMutations int                `json:"seeded_mutations"`
	StartedAt       time.Time          `json:"started_at"`
	EndedAt         time.Time          `json:"ended_at"`
}

func main() {
	var o options
	flag.StringVar(&o.endpoints, "endpoints", "", "three explicitly owned HTTPS endpoints")
	flag.StringVar(&o.ca, "ca", "", "trusted fixture CA")
	flag.StringVar(&o.cert, "cert", "", "fixture client certificate")
	flag.StringVar(&o.key, "key", "", "fixture client private key")
	flag.StringVar(&o.output, "output", "", "new private JSON report path")
	flag.StringVar(&o.engine, "engine", "", "candidate/reference build identity")
	flag.IntVar(&o.keys, "keys", 100_000, "synthetic live keys")
	flag.IntVar(&o.mutations, "mutations", 1_000_000, "durable pre-measurement mutations")
	flag.IntVar(&o.concurrency, "concurrency", 32, "bounded client workers")
	flag.DurationVar(&o.warmup, "warmup", 5*time.Minute, "unmeasured exact-mix warmup")
	flag.DurationVar(&o.duration, "duration", 15*time.Minute, "measured steady state")
	flag.Int64Var(&o.seed, "seed", 42, "synthetic workload seed")
	flag.BoolVar(&o.owned, "owned-fixture", false, "attest endpoints are isolated owned synthetic fixtures")
	flag.BoolVar(&o.correctness, "correctness", false, "collect bounded synthetic external histories instead of primary performance")
	flag.StringVar(&o.checkHistory, "check-history", "", "check a previously captured private synthetic history without contacting servers")
	flag.Parse()
	var err error
	switch {
	case o.checkHistory != "":
		err = checkHistoryFile(o.checkHistory)
	case o.correctness:
		err = runCorrectness(o)
	default:
		err = run(o)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "qualification:", err)
		os.Exit(1)
	}
}

func fixtureClient(o options) (*clientv3.Client, error) {
	endpoints := strings.Split(o.endpoints, ",")
	if len(endpoints) != 3 {
		return nil, errors.New("qualification requires exactly three HTTPS endpoints")
	}
	for _, endpoint := range endpoints {
		if !strings.HasPrefix(endpoint, "https://") {
			return nil, errors.New("qualification never weakens the TLS posture")
		}
	}
	certificate, err := tls.LoadX509KeyPair(o.cert, o.key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(o.ca)
	if err != nil {
		return nil, err
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid fixture CA")
	}
	config := clientv3.Config{
		Endpoints: endpoints, DialTimeout: 5 * time.Second,
		TLS:         &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: ca, Certificates: []tls.Certificate{certificate}},
		DialOptions: []grpc.DialOption{grpc.WithNoProxy()}, Logger: zap.NewNop(),
	}
	if o.correctness {
		config.MaxUnaryRetries = 1
		config.MaxCallRecvMsgSize = maxHistoryRecordBytes
	}
	return clientv3.New(config)
}

func operation(index int) string {
	switch {
	case index < 60:
		return "range"
	case index < 70:
		return "prefix-range"
	case index < 90:
		return "put"
	default:
		return "cas"
	}
}

func key(index int) string {
	prefix := fmt.Sprintf("%skey/%04d/%d/", namespace, index/10, index%10)
	return prefix + strings.Repeat("k", 64-len(prefix))
}

func prefix(index int) string {
	return fmt.Sprintf("%skey/%04d/", namespace, index/10)
}

func run(o options) (result error) {
	if !o.owned || o.output == "" || o.engine == "" || o.keys < 10 || o.keys > 100_000 || o.keys%10 != 0 ||
		o.mutations < o.keys || o.concurrency < 1 || o.concurrency > 512 || o.warmup < 0 || o.duration <= 0 {
		return errors.New("qualification requires an owned fixture, output/build identity, bounded ten-key groups, complete seeding and bounded positive timing")
	}
	endpoints := strings.Split(o.endpoints, ",")
	if len(endpoints) != 3 {
		return errors.New("qualification requires exactly three HTTPS endpoints")
	}
	for _, endpoint := range endpoints {
		if !strings.HasPrefix(endpoint, "https://") {
			return errors.New("qualification never weakens the TLS posture")
		}
	}
	file, err := os.OpenFile(o.output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	r := report{
		Format: 2, Engine: o.engine, Seed: o.seed, Keys: o.keys, Mutations: o.mutations,
		KeyBytes: 64, ValueBytes: 1024, Concurrency: o.concurrency, PerOperation: make(map[string]latency),
		ErrorDetails: []requestError{}, WarmupDetails: []requestError{}, WatchDetails: []requestError{},
		ClientOS: runtime.GOOS, ClientArch: runtime.GOARCH, ClientMaxProcs: runtime.GOMAXPROCS(0),
		Consistency: "durable-quorum/default-linearizable", SampleLimit: maxLatencySamples,
		WatchShape: "100 disjoint prefixes covering 10% of 100000 keys in the primary shape",
		Phase:      "tls", StartedAt: time.Now().UTC(),
	}
	var watchEvidence errorEvidence
	var watchCaptureMu sync.Mutex
	var watchCaptureError error
	defer func() {
		r.EndedAt = time.Now().UTC()
		r.WatchErrors, r.WatchDetails = watchEvidence.snapshot()
		if result != nil {
			r.Failure = result.Error()
		}
		writeError := json.NewEncoder(file).Encode(r)
		if writeError == nil {
			writeError = file.Sync()
		}
		result = errors.Join(result, writeError, file.Close())
	}()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	client, err := fixtureClient(o)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, client.Close()) }()
	r.Phase = "status"
	versions := make([]string, 0, 3)
	for _, endpoint := range endpoints {
		call, done := context.WithTimeout(ctx, 10*time.Second)
		status, err := client.Status(call, endpoint)
		done()
		if err != nil {
			return fmt.Errorf("fixture status: %w", err)
		}
		versions = append(versions, status.Version)
	}
	r.ServerVersions = versions
	call, done := context.WithTimeout(ctx, 10*time.Second)
	existing, err := client.Get(call, namespace, clientv3.WithPrefix(), clientv3.WithCountOnly())
	done()
	if err != nil {
		return err
	}
	if existing.Count != 0 {
		return errors.New("fixture namespace is not empty; qualification never deletes or reuses existing data")
	}
	value := strings.Repeat("v", 1024)
	r.Phase = "seed"
	for start := 0; start < o.mutations; {
		size := min(128, o.mutations-start)
		ops := make([]clientv3.Op, size)
		for index := range ops {
			ops[index] = clientv3.OpPut(key((start+index)%o.keys), value)
		}
		call, done := context.WithTimeout(ctx, 30*time.Second)
		_, err := client.Txn(call).Then(ops...).Commit()
		done()
		if err != nil {
			return fmt.Errorf("durable seed batch %d: %w", start, err)
		}
		start += size
		r.SeededMutations = start
	}
	var watchWG sync.WaitGroup
	defer func() {
		cancel()
		watchWG.Wait()
		watchCaptureMu.Lock()
		result = errors.Join(result, watchCaptureError)
		watchCaptureMu.Unlock()
	}()
	watchStarted := time.Now()
	recordWatchError := func(failure error) {
		if err := watchEvidence.record("watch", failure, time.Since(watchStarted).Nanoseconds()); err != nil {
			watchCaptureMu.Lock()
			watchCaptureError = errors.Join(watchCaptureError, err)
			watchCaptureMu.Unlock()
			cancel()
		}
	}
	watchCount := 100
	if o.keys < 100_000 {
		watchCount = max(1, o.keys/1_000)
	}
	r.Watches = watchCount
	for index := 0; index < watchCount; index++ {
		watchWG.Add(1)
		stream := client.Watch(ctx, fmt.Sprintf("%skey/%03d", namespace, index), clientv3.WithPrefix(), clientv3.WithProgressNotify())
		go func() {
			defer watchWG.Done()
			var revision int64
			for response := range stream {
				if response.Err() != nil {
					if ctx.Err() == nil {
						recordWatchError(response.Err())
					}
					return
				}
				for _, event := range response.Events {
					if event == nil || event.Kv == nil {
						recordWatchError(errors.New("watch returned an invalid event shape"))
						return
					}
					if event.Kv.ModRevision < revision {
						recordWatchError(fmt.Errorf("watch revision regressed from %d to %d", revision, event.Kv.ModRevision))
					}
					revision = event.Kv.ModRevision
				}
			}
			if ctx.Err() == nil {
				recordWatchError(errors.New("watch stream ended without cancellation"))
			}
		}()
	}
	r.Phase = "warmup"
	if o.warmup > 0 {
		warmup, err := workload(ctx, client, o, o.warmup, false)
		r.WarmupErrors, r.WarmupSeconds, r.WarmupDetails = warmup.Errors, warmup.Elapsed.Seconds(), warmup.ErrorDetails
		if err != nil {
			return err
		}
	}
	r.Phase = "measurement"
	measured, err := workload(ctx, client, o, o.duration, true)
	r.Errors, r.MeasuredSeconds, r.ErrorDetails = measured.Errors, measured.Elapsed.Seconds(), measured.ErrorDetails
	cancel()
	watchWG.Wait()
	var count int
	for name, values := range measured.Samples {
		count += len(values)
		r.PerOperation[name] = percentile(values)
	}
	if measured.Elapsed > 0 {
		r.Throughput = float64(count) / measured.Elapsed.Seconds()
	}
	if err != nil {
		return fmt.Errorf("incomplete measurement retained: %w", err)
	}
	r.Completed = true
	r.Phase = "complete"
	r.PrimaryShape = o.keys == 100_000 && o.mutations == 1_000_000 && watchCount == 100 &&
		r.WarmupSeconds >= (5*time.Minute).Seconds() && r.MeasuredSeconds >= (15*time.Minute).Seconds()
	watchErrors, _ := watchEvidence.snapshot()
	if measured.Errors != 0 || watchErrors != 0 || r.WarmupErrors != 0 {
		return fmt.Errorf("qualification retained failed evidence: request errors=%d, warmup errors=%d, watch errors=%d", measured.Errors, r.WarmupErrors, watchErrors)
	}
	return nil
}

type workloadResult struct {
	Samples      map[string][]int64
	Errors       int64
	ErrorDetails []requestError
	Elapsed      time.Duration
}

func workload(ctx context.Context, client clientv3.KV, o options, duration time.Duration, record bool) (workloadResult, error) {
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	deadline := start.Add(duration)
	var mu sync.Mutex
	samples := map[string][]int64{"range": {}, "prefix-range": {}, "put": {}, "cas": {}}
	var evidence errorEvidence
	var count atomic.Int64
	var fatal error
	var workers sync.WaitGroup
	for worker := 0; worker < o.concurrency; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			random := rand.New(rand.NewSource(o.seed + int64(worker)))
			for time.Now().Before(deadline) && ctx.Err() == nil {
				for index := 0; index < 100; index++ {
					if ctx.Err() != nil {
						break
					}
					name := operation(index)
					item := random.Intn(o.keys)
					call, done := context.WithTimeout(ctx, 5*time.Second)
					begin := time.Now()
					var err error
					switch name {
					case "range":
						var response *clientv3.GetResponse
						response, err = client.Get(call, key(item))
						if err == nil && (len(response.Kvs) != 1 || string(response.Kvs[0].Value) != strings.Repeat("v", 1024)) {
							err = errors.New("range did not return its seeded synthetic value")
						}
					case "prefix-range":
						var response *clientv3.GetResponse
						response, err = client.Get(call, prefix(item), clientv3.WithPrefix())
						if err == nil && len(response.Kvs) != 10 {
							err = errors.New("prefix range did not return its exact ten-key group")
						}
					case "put":
						_, err = client.Put(call, key(item), strings.Repeat("v", 1024))
					case "cas":
						var response *clientv3.TxnResponse
						response, err = client.Txn(call).If(clientv3.Compare(clientv3.Value(key(item)), "=", strings.Repeat("v", 1024))).
							Then(clientv3.OpPut(key(item), strings.Repeat("v", 1024))).Commit()
						if err == nil && !response.Succeeded {
							err = errors.New("CAS rejected the seeded synthetic value")
						}
					}
					latencyNS := time.Since(begin).Nanoseconds()
					done()
					if err != nil {
						if captureError := evidence.record(name, err, time.Since(start).Nanoseconds()); captureError != nil {
							mu.Lock()
							fatal = errors.Join(fatal, captureError)
							mu.Unlock()
							cancel()
						}
					}
					if record {
						mu.Lock()
						if count.Add(1) <= maxLatencySamples {
							samples[name] = append(samples[name], latencyNS)
						} else {
							fatal = errors.Join(fatal, errors.New("exact latency sample capacity exceeded; no sampled percentile claim is emitted"))
							cancel()
						}
						mu.Unlock()
					}
				}
			}
		}(worker)
	}
	workers.Wait()
	if fatal != nil {
		samples = nil
	}
	failures, details := evidence.snapshot()
	return workloadResult{Samples: samples, Errors: failures, ErrorDetails: details, Elapsed: time.Since(start)}, errors.Join(fatal, parent.Err())
}

func percentile(values []int64) latency {
	if len(values) == 0 {
		return latency{}
	}
	slices.Sort(values)
	at := func(percent float64) float64 {
		index := max(0, int(math.Ceil(percent*float64(len(values))))-1)
		return float64(values[index]) / 1_000
	}
	return latency{Count: len(values), P50: at(.5), P95: at(.95), P99: at(.99), P999Us: at(.999)}
}
