package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequestErrorEvidenceRetainsOperationCodeMessageCountAndTiming(t *testing.T) {
	var evidence errorEvidence
	failure := status.Error(codes.Unavailable, "mock peer unavailable")
	for _, entry := range []struct {
		operation string
		time      int64
	}{{"range", 20}, {"range", 10}, {"put", 30}} {
		if err := evidence.record(entry.operation, failure, entry.time); err != nil {
			t.Fatal(err)
		}
	}
	count, details := evidence.snapshot()
	if count != 3 || len(details) != 2 {
		t.Fatalf("lost request errors: count=%d details=%+v", count, details)
	}
	for _, detail := range details {
		if detail.Code != "Unavailable" || detail.Message != failure.Error() {
			t.Fatalf("lost exact error identity: %+v", detail)
		}
		if detail.Operation == "range" && (detail.Count != 2 || detail.FirstNS != 10 || detail.LastNS != 20) {
			t.Fatalf("lost repeated error timing: %+v", detail)
		}
	}
	details[0].Count = 999
	_, unchanged := evidence.snapshot()
	if unchanged[0].Count == 999 {
		t.Fatal("snapshot aliases the live error evidence")
	}
}

func TestRequestErrorEvidenceDoesNotReclassifyContextOrValidationFailures(t *testing.T) {
	var evidence errorEvidence
	for _, failure := range []error{
		fmt.Errorf("mock request: %w", context.DeadlineExceeded),
		fmt.Errorf("mock request: %w", context.Canceled),
		errors.New("mock range returned a wrong value"),
	} {
		if err := evidence.record("range", failure, 1); err != nil {
			t.Fatal(err)
		}
	}
	_, details := evidence.snapshot()
	found := make(map[string]bool)
	for _, detail := range details {
		found[detail.Code] = true
	}
	if !found["DeadlineExceeded"] || !found["Canceled"] || !found["Unknown"] {
		t.Fatalf("error codes were normalized away: %+v", details)
	}
}

func TestRequestErrorEvidenceFailsExplicitlyAtItsBounds(t *testing.T) {
	var evidence errorEvidence
	for index := 0; index < maxErrorDetails; index++ {
		if err := evidence.record("range", fmt.Errorf("mock failure %d", index), int64(index)); err != nil {
			t.Fatal(err)
		}
	}
	if err := evidence.record("range", errors.New("mock additional failure"), 1000); err == nil {
		t.Fatal("distinct error evidence was silently dropped")
	}
	count, details := evidence.snapshot()
	if count != int64(maxErrorDetails+1) || len(details) != maxErrorDetails {
		t.Fatal("overflow lost the total request-error count")
	}
	var oversized errorEvidence
	if err := oversized.record("put", errors.New(strings.Repeat("x", maxErrorMessageBytes+1)), 1); err == nil {
		t.Fatal("oversized error evidence was silently truncated")
	}
	count, _ = oversized.snapshot()
	if count != 1 {
		t.Fatal("oversized failure was not counted")
	}
}

func TestRequestErrorEvidenceIsRaceSafeAndRejectsInvalidObservations(t *testing.T) {
	var evidence errorEvidence
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := 0; index < 100; index++ {
				if err := evidence.record("watch", errors.New("mock watch disconnected"), int64(index)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	workers.Wait()
	count, details := evidence.snapshot()
	if count != 800 || len(details) != 1 || details[0].Count != 800 {
		t.Fatalf("concurrent failures were lost: %d %+v", count, details)
	}
	if err := evidence.record("range", nil, 0); err == nil {
		t.Fatal("successful request was accepted as failed")
	}
	if err := evidence.record("range", errors.New("mock failure"), -1); err == nil {
		t.Fatal("invalid monotonic timing was accepted")
	}
}
