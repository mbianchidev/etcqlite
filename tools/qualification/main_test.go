package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failedRangeFixture struct {
	clientv3.KV
	cancel   context.CancelFunc
	deadline time.Duration
	failure  error
}

func (f *failedRangeFixture) Get(ctx context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	deadline, present := ctx.Deadline()
	if !present {
		return nil, errors.New("mock request has no deadline")
	}
	f.deadline = time.Until(deadline)
	if f.cancel != nil {
		f.cancel()
	}
	if f.failure != nil {
		return nil, f.failure
	}
	return nil, status.Error(codes.Unavailable, "mock peer unreachable")
}

func TestWorkloadCapturesErrorsDuringWarmupAndMeasurementWithoutChangingDeadlines(t *testing.T) {
	for _, record := range []bool{false, true} {
		t.Run(map[bool]string{false: "warmup", true: "measurement"}[record], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture := &failedRangeFixture{cancel: cancel}
			result, err := workload(ctx, fixture, options{keys: 10, concurrency: 1, seed: 42}, time.Minute, record)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation was hidden: %v", err)
			}
			if fixture.deadline <= 4*time.Second || fixture.deadline > 5*time.Second {
				t.Fatalf("request deadline changed: %s", fixture.deadline)
			}
			if result.Errors != 1 || len(result.ErrorDetails) != 1 {
				t.Fatalf("request failure was lost: %+v", result)
			}
			detail := result.ErrorDetails[0]
			if detail.Operation != "range" || detail.Code != "Unavailable" || detail.Count != 1 ||
				detail.Message != status.Error(codes.Unavailable, "mock peer unreachable").Error() ||
				detail.FirstNS < 0 || detail.LastNS > result.Elapsed.Nanoseconds() {
				t.Fatalf("request failure evidence differs: %+v", detail)
			}
			if record && len(result.Samples["range"]) != 1 {
				t.Fatal("failed measured request lost its exact latency")
			}
			if !record && len(result.Samples["range"]) != 0 {
				t.Fatal("warmup was added to measured latency samples")
			}
		})
	}
}

func TestWorkloadStopsWhenFailureEvidenceWouldBeTruncated(t *testing.T) {
	fixture := &failedRangeFixture{failure: errors.New(strings.Repeat("x", maxErrorMessageBytes+1))}
	result, err := workload(context.Background(), fixture, options{keys: 10, concurrency: 1}, time.Minute, true)
	if err == nil || !strings.Contains(err.Error(), "request-error message exceeds") {
		t.Fatalf("evidence overflow did not stop the workload explicitly: %v", err)
	}
	if result.Errors != 1 || result.Samples != nil {
		t.Fatalf("overflow published successful-looking samples or lost the failure: %+v", result)
	}
}

func TestFailedSetupRetainsTheNewReportShapeInAPrivateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mock-report.json")
	err := run(options{
		owned: true, output: path, engine: "mock-build", keys: 10, mutations: 10,
		concurrency: 1, duration: time.Millisecond,
		endpoints: "https://mock-one.invalid:2379,https://mock-two.invalid:2379,https://mock-three.invalid:2379",
		cert:      filepath.Join(t.TempDir(), "mock-missing.crt"),
	})
	if err == nil {
		t.Fatal("failed fixture setup appeared successful")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded["format"]) != "2" || string(decoded["completed"]) != "false" || len(decoded["failure"]) == 0 {
		t.Fatal("failed setup report lost its version or incomplete outcome")
	}
	for _, name := range []string{"error_details", "warmup_error_details", "watch_error_details"} {
		if string(decoded[name]) != "[]" {
			t.Fatalf("zero-error evidence must remain an explicit array: %s=%s", name, decoded[name])
		}
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("failure report is not private: %v %v", info, err)
	}
}
