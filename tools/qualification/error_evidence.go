package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxErrorDetails      = 128
	maxErrorMessageBytes = 4096
)

type requestError struct {
	Operation string `json:"operation"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Count     int64  `json:"count"`
	FirstNS   int64  `json:"first_ns"`
	LastNS    int64  `json:"last_ns"`
}

type errorEvidence struct {
	mu      sync.Mutex
	count   int64
	details []requestError
}

func (e *errorEvidence) record(operation string, failure error, elapsedNS int64) error {
	if failure == nil || elapsedNS < 0 || operation == "" || len(operation) > 64 {
		return errors.New("invalid request-error observation")
	}
	code := status.Code(failure)
	if errors.Is(failure, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	} else if errors.Is(failure, context.Canceled) {
		code = codes.Canceled
	}
	message := failure.Error()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.count++
	if len(message) > maxErrorMessageBytes {
		return fmt.Errorf("request-error message exceeds %d bytes (operation=%s, code=%s)", maxErrorMessageBytes, operation, code)
	}
	for index := range e.details {
		detail := &e.details[index]
		if detail.Operation == operation && detail.Code == code.String() && detail.Message == message {
			detail.Count++
			detail.FirstNS = min(detail.FirstNS, elapsedNS)
			detail.LastNS = max(detail.LastNS, elapsedNS)
			return nil
		}
	}
	if len(e.details) >= maxErrorDetails {
		return fmt.Errorf("request-error evidence exceeds %d distinct details (operation=%s, code=%s)", maxErrorDetails, operation, code)
	}
	e.details = append(e.details, requestError{
		Operation: operation, Code: code.String(), Message: message, Count: 1,
		FirstNS: elapsedNS, LastNS: elapsedNS,
	})
	return nil
}

func (e *errorEvidence) snapshot() (int64, []requestError) {
	e.mu.Lock()
	defer e.mu.Unlock()
	details := make([]requestError, len(e.details))
	copy(details, e.details)
	slices.SortFunc(details, func(left, right requestError) int {
		if order := strings.Compare(left.Operation, right.Operation); order != 0 {
			return order
		}
		if order := strings.Compare(left.Code, right.Code); order != 0 {
			return order
		}
		return strings.Compare(left.Message, right.Message)
	})
	return e.count, details
}
