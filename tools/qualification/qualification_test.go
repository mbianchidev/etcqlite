package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPrimaryRequestMixAndBinaryShapesAreExact(t *testing.T) {
	counts := make(map[string]int)
	for index := 0; index < 100; index++ {
		counts[operation(index)]++
	}
	for name, expected := range map[string]int{"range": 60, "prefix-range": 10, "put": 20, "cas": 10} {
		if counts[name] != expected {
			t.Fatalf("%s count=%d, want%d", name, counts[name], expected)
		}
	}
	for _, index := range []int{0, 9, 10, 99_999} {
		if len(key(index)) != 64 {
			t.Fatal("primary key is not exactly64bytes")
		}
	}
	if prefix(0) != prefix(9) || prefix(9) == prefix(10) {
		t.Fatal("prefix groups are not exactly ten keys")
	}
}

func TestExactPercentilesDoNotHideTailLatency(t *testing.T) {
	values := []int64{1_000, 5_000, 2_000, 3_000, 4_000}
	result := percentile(values)
	if result.P50 != 3 || result.P99 != 5 || result.Count != 5 {
		t.Fatalf("wrong exact percentiles: %+v", result)
	}
}

func TestCancellationStopsBeforeIssuingAnotherRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	samples, failures, _, err := workload(ctx, nil, options{concurrency: 4}, time.Minute, true)
	if !errors.Is(err, context.Canceled) || failures != 0 {
		t.Fatalf("cancelled workload fabricated requests: failures=%d error=%v", failures, err)
	}
	for _, values := range samples {
		if len(values) != 0 {
			t.Fatal("cancelled workload recorded nonexistent operations")
		}
	}
}

func TestFailedSetupPreservesPrivateEvidenceAndDoesNotOverwriteIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failure.json")
	o := options{
		endpoints: "https://mock1:2379,https://mock2:2379,https://mock3:2379",
		keys:      10, mutations: 10, concurrency: 1, duration: time.Second, engine: "mock-build", owned: true, output: path,
	}
	if err := run(o); err == nil {
		t.Fatal("missing fixture TLS should fail")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result report
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Failure == "" || result.Completed || result.PrimaryShape || result.Phase != "tls" {
		t.Fatalf("failure looks successful: %+v", result)
	}
	if err := run(o); err == nil {
		t.Fatal("existing evidence must not be overwritten")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, data) {
		t.Fatal("existing evidence changed")
	}
}
