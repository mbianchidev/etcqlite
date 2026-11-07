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
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	flag.Parse()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "qualification:", err)
		os.Exit(1)
	}
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
	certificate, err := tls.LoadX509KeyPair(o.cert, o.key)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(o.ca)
	if err != nil {
		return err
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(data) {
		return errors.New("invalid fixture CA")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints, DialTimeout: 5 * time.Second,
		TLS:         &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: ca, Certificates: []tls.Certificate{certificate}},
		DialOptions: []grpc.DialOption{grpc.WithNoProxy()}, Logger: zap.NewNop(),
	})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, client.Close()) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	}
	var watchErrors atomic.Int64
	var watchWG sync.WaitGroup
	watchCount := 100
	if o.keys < 100_000 {
		watchCount = max(1, o.keys/1_000)
	}
	for index := 0; index < watchCount; index++ {
		watchWG.Add(1)
		stream := client.Watch(ctx, fmt.Sprintf("%skey/%03d", namespace, index), clientv3.WithPrefix(), clientv3.WithProgressNotify())
		go func() {
			defer watchWG.Done()
			var revision int64
			for response := range stream {
				if response.Err() != nil {
					if ctx.Err() == nil {
						watchErrors.Add(1)
					}
					return
				}
				for _, event := range response.Events {
					if event.Kv.ModRevision < revision {
						watchErrors.Add(1)
					}
					revision = event.Kv.ModRevision
				}
			}
		}()
	}
	var warmupErrors int64
	if o.warmup > 0 {
		if _, warmupFailures, _, err := workload(ctx, client, o, o.warmup, false); err != nil {
			return err
		} else {
			warmupErrors = warmupFailures
		}
	}
	samples, failures, elapsed, err := workload(ctx, client, o, o.duration, true)
	if err != nil {
		return err
	}
	cancel()
	watchWG.Wait()
	r := report{
		Format: 1, Engine: o.engine, Seed: o.seed, Keys: o.keys, Mutations: o.mutations,
		KeyBytes: 64, ValueBytes: 1024, Concurrency: o.concurrency, Watches: watchCount,
		WarmupSeconds: o.warmup.Seconds(), MeasuredSeconds: elapsed.Seconds(), Errors: failures,
		WatchErrors: watchErrors.Load(), PerOperation: make(map[string]latency),
		WarmupErrors:   warmupErrors,
		ServerVersions: versions, ClientOS: runtime.GOOS, ClientArch: runtime.GOARCH,
		ClientMaxProcs: runtime.GOMAXPROCS(0), ProductionClaim: false,
		Consistency: "durable-quorum/default-linearizable", SampleLimit: maxLatencySamples,
		WatchShape: "100 disjoint prefixes covering 10% of 100000 keys in the primary shape",
		PrimaryShape: o.keys == 100_000 && o.mutations == 1_000_000 && watchCount == 100 &&
			o.warmup >= 5*time.Minute && o.duration >= 15*time.Minute,
	}
	var count int
	for name, values := range samples {
		count += len(values)
		r.PerOperation[name] = percentile(values)
	}
	r.Throughput = float64(count) / elapsed.Seconds()
	file, err := os.OpenFile(o.output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(file).Encode(r)
	if err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	if failures != 0 || r.WatchErrors != 0 || warmupErrors != 0 {
		return fmt.Errorf("qualification retained failed evidence: request errors=%d, warmup errors=%d, watch errors=%d", failures, warmupErrors, r.WatchErrors)
	}
	return nil
}

func workload(ctx context.Context, client *clientv3.Client, o options, duration time.Duration, record bool) (map[string][]int64, int64, time.Duration, error) {
	start := time.Now()
	deadline := start.Add(duration)
	var mu sync.Mutex
	samples := map[string][]int64{"range": {}, "prefix-range": {}, "put": {}, "cas": {}}
	var failures, count atomic.Int64
	var fatal error
	var workers sync.WaitGroup
	for worker := 0; worker < o.concurrency; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			random := rand.New(rand.NewSource(o.seed + int64(worker)))
			for time.Now().Before(deadline) && ctx.Err() == nil {
				for index := 0; index < 100; index++ {
					name := operation(index)
					item := random.Intn(o.keys)
					call, done := context.WithTimeout(ctx, 5*time.Second)
					begin := time.Now()
					var err error
					switch name {
					case "range":
						_, err = client.Get(call, key(item))
					case "prefix-range":
						var response *clientv3.GetResponse
						response, err = client.Get(call, prefix(item), clientv3.WithPrefix())
						if err == nil && len(response.Kvs) != 10 {
							err = errors.New("prefix range did not return its exact ten-key group")
						}
					case "put":
						_, err = client.Put(call, key(item), strings.Repeat("v", 1024))
					case "cas":
						_, err = client.Txn(call).If(clientv3.Compare(clientv3.Value(key(item)), "=", strings.Repeat("v", 1024))).
							Then(clientv3.OpPut(key(item), strings.Repeat("v", 1024))).Commit()
					}
					done()
					if err != nil {
						failures.Add(1)
					}
					if record {
						mu.Lock()
						if count.Add(1) <= maxLatencySamples {
							samples[name] = append(samples[name], time.Since(begin).Nanoseconds())
						} else {
							fatal = errors.New("exact latency sample capacity exceeded; no sampled percentile claim is emitted")
						}
						mu.Unlock()
					}
				}
			}
		}(worker)
	}
	workers.Wait()
	return samples, failures.Load(), time.Since(start), fatal
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
