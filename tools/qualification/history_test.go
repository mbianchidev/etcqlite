package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func TestExternalHistoryOracleRejectsStaleReadsCASAndTornTransactions(t *testing.T) {
	for _, history := range [][]historyOperation{
		{{Call: 0, Return: 1, Input: historyInput{Kind: "write", Value: "mock-new"}, Revision: 2}, {Call: 2, Return: 3, Input: historyInput{Kind: "read"}, Output: historyOutput{Value: ""}, Revision: 2}},
		{{Call: 0, Return: 3, Input: historyInput{Kind: "cas", Value: "mock-one"}, Output: historyOutput{Succeeded: true}, Revision: 2}, {Client: 1, Call: 1, Return: 2, Input: historyInput{Kind: "cas", Value: "mock-two"}, Output: historyOutput{Succeeded: true}, Revision: 2}},
		{{Call: 0, Return: 1, Input: historyInput{Kind: "txn-write", Value: "mock-pair"}, Revision: 2}, {Call: 2, Return: 3, Input: historyInput{Kind: "txn-read"}, Output: historyOutput{Pair: [2]string{"mock-pair", ""}}, Revision: 2}},
	} {
		result, err := checkHistory(history, time.Second)
		if err == nil || result != porcupine.Illegal {
			t.Fatalf("invalid history accepted: result=%s error=%v", result, err)
		}
	}
}

func TestExternalHistoryOraclePreservesUncertainWritesAndDisjointGroups(t *testing.T) {
	history := []historyOperation{
		{Call: 0, Return: 1, Input: historyInput{Kind: "write", Value: "mock-uncertain"}, Output: historyOutput{Uncertain: true}, Error: "mock unavailable"},
		{Call: 2, Return: 3, Input: historyInput{Kind: "read"}, Output: historyOutput{Value: "mock-uncertain"}, Revision: 2},
		{Call: 4, Return: 5, Input: historyInput{Kind: "write", Value: "mock-other", Group: 1}, Revision: 3},
		{Call: 6, Return: 7, Input: historyInput{Kind: "read", Group: 1}, Output: historyOutput{Value: "mock-other"}, Revision: 3},
	}
	if result, err := checkHistory(history, time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("valid uncertain history rejected: %v %v", result, err)
	}
}

func TestExternalHistoryOracleChecksAppendAndSetWithoutMutatingState(t *testing.T) {
	history := []historyOperation{
		{Call: 0, Return: 1, Input: historyInput{Kind: "append", Value: "mock-a"}, Output: historyOutput{Value: `["mock-a"]`}, Revision: 2},
		{Call: 2, Return: 3, Input: historyInput{Kind: "append", Value: "mock-b"}, Output: historyOutput{Value: `["mock-a","mock-b"]`}, Revision: 3},
		{Call: 4, Return: 5, Input: historyInput{Kind: "list-read"}, Output: historyOutput{Value: `["mock-a","mock-b"]`}, Revision: 3},
		{Call: 6, Return: 7, Input: historyInput{Kind: "set-add", Value: "mock-a"}, Revision: 4},
		{Call: 8, Return: 9, Input: historyInput{Kind: "set-add", Value: "mock-a"}, Revision: 5},
		{Call: 10, Return: 11, Input: historyInput{Kind: "set-read"}, Output: historyOutput{Value: `["mock-a"]`}, Revision: 5},
	}
	if _, err := checkHistory(history, time.Second); err != nil {
		t.Fatal(err)
	}
	history[2].Output.Value = `["mock-b","mock-a"]`
	if _, err := checkHistory(history, time.Second); err == nil {
		t.Fatal("reordered append history accepted")
	}
}

func TestExternalHistoryOracleRejectsAnIncorrectAppendResponse(t *testing.T) {
	history := []historyOperation{
		{Call: 1, Return: 2, Input: historyInput{Kind: "append", Value: "mock-a"}, Output: historyOutput{Value: `["mock-invented"]`}, Revision: 2},
	}
	if _, err := checkHistory(history, time.Second); err == nil {
		t.Fatal("incorrect acknowledged append result accepted")
	}
}

func TestWatchOracleRejectsInventedAndReorderedEvents(t *testing.T) {
	prefix := "mock/robustness/42/00000000/"
	history := []historyOperation{
		{Call: 1, Return: 2, Input: historyInput{Kind: "write", Value: "mock-a"}, Revision: 2},
		{Call: 3, Return: 4, Input: historyInput{Kind: "write", Value: "mock-b"}, Revision: 3},
	}
	first := watchEntry{watchKV: watchKV{Key: prefix + "0/register", Value: "mock-a", Revision: 2, CreateRevision: 2, Version: 1}}
	second := watchEntry{watchKV: watchKV{Key: prefix + "0/register", Value: "mock-b", Revision: 3, CreateRevision: 2, Version: 2}, PrevKV: &first.watchKV}
	for _, events := range [][]watchEntry{
		{first, second, {watchKV: watchKV{Key: prefix + "0/invented", Value: "mock-extra", Revision: 4, CreateRevision: 4, Version: 1}}},
		{second, first},
	} {
		if err := validateWatch(historyRecord{Prefix: prefix, StartRevision: 1, EndRevision: 4, Operations: history, Watch: events, FinalKV: []watchKV{second.watchKV}}); err == nil {
			t.Fatal("invalid watch evidence accepted")
		}
	}
}

func TestHistoryFileRejectsMissingManifestRepeatedChecksAndTrailingRecords(t *testing.T) {
	prefix := "mock/robustness/42/00000000/"
	manifest := historyRecord{Kind: "manifest", Format: historyFormat, Engine: "mock-build", Seed: 42, Versions: []string{"3.7.3", "3.7.3", "3.7.3"}, Prefix: "mock/robustness/42/", DurationNS: 1}
	kv := watchKV{Key: prefix + "0/register", Value: "mock-a", Revision: 2, CreateRevision: 2, Version: 1}
	epoch := historyRecord{Kind: "epoch", Format: historyFormat, Prefix: prefix, StartRevision: 1, EndRevision: 2, FinalKV: []watchKV{kv}, Operations: []historyOperation{
		{Call: 1, Return: 2, Input: historyInput{Kind: "write", Value: "mock-a"}, Revision: 2},
	}, Watch: []watchEntry{{watchKV: kv}}}
	check := historyRecord{Kind: "check", Format: historyFormat, Check: "Ok"}
	complete := historyRecord{Kind: "complete", Format: historyFormat, Epoch: 1, ElapsedNS: 10, Check: "Ok"}
	for _, records := range [][]historyRecord{
		{epoch, check, complete},
		{manifest, epoch, check, check, complete},
		{manifest, epoch, check, complete, check},
		{manifest, epoch, complete},
	} {
		path := filepath.Join(t.TempDir(), "mock-history.jsonl")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if err := json.NewEncoder(file).Encode(record); err != nil {
				t.Fatal(err)
			}
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if err := checkHistoryFile(path); err == nil {
			t.Fatal("malformed history record sequence accepted")
		}
	}
	path := filepath.Join(t.TempDir(), "mock-valid-history.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []historyRecord{manifest, epoch, check, complete} {
		if err := writeHistoryRecord(file, record); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := checkHistoryFile(path); err != nil {
		t.Fatalf("valid checked stream rejected: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, malformed := range map[string]string{
		"pair shape":   strings.ReplaceAll(string(data), `"pair":["",""]`, `"pair":["","","mock-extra"]`),
		"client field": strings.ReplaceAll(string(data), `"client":0,`, ""),
		"group field":  strings.ReplaceAll(string(data), `"group":0,`, ""),
		"KV metadata":  strings.ReplaceAll(string(data), `"lease":0,`, ""),
	} {
		t.Run(name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "mock-malformed.jsonl")
			if err := os.WriteFile(file, []byte(malformed), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := checkHistoryFile(file); err == nil {
				t.Fatal("incomplete or truncated response shape accepted")
			}
		})
	}
}

func TestWatchOracleChecksPreviousMetadataFinalStateAndUncertainEffects(t *testing.T) {
	prefix := "mock/robustness/42/00000000/"
	first := watchKV{Key: prefix + "0/register", Value: "mock-a", Revision: 2, CreateRevision: 2, Version: 1}
	second := watchKV{Key: first.Key, Value: first.Value, Revision: 3, CreateRevision: 2, Version: 2}
	record := historyRecord{Prefix: prefix, StartRevision: 1, EndRevision: 2,
		Operations: []historyOperation{{Call: 1, Return: 2, Input: historyInput{Kind: "write", Value: "mock-a"}, Output: historyOutput{Uncertain: true}, Error: "mock unavailable"}},
		Watch:      []watchEntry{{watchKV: first}}, FinalKV: []watchKV{first},
	}
	if err := validateWatch(record); err != nil {
		t.Fatal(err)
	}
	record.EndRevision = 3
	record.Watch = append(record.Watch, watchEntry{watchKV: second, PrevKV: &first})
	record.FinalKV = []watchKV{second}
	if err := validateWatch(record); err == nil {
		t.Fatal("one uncertain invocation explained multiple committed writes")
	}
	record.Operations = append(record.Operations, historyOperation{Call: 3, Return: 4, Input: historyInput{Kind: "write", Value: "mock-a"}, Revision: 3})
	if err := validateWatch(record); err != nil {
		t.Fatal(err)
	}
	record.FinalKV[0].Version = 99
	if err := validateWatch(record); err == nil {
		t.Fatal("altered final metadata accepted")
	}
	record.FinalKV[0] = second
	record.Watch[1].PrevKV = &watchKV{Key: first.Key, Value: "mock-wrong", Revision: 2, CreateRevision: 2, Version: 1}
	if err := validateWatch(record); err == nil {
		t.Fatal("altered previous value accepted")
	}
}

func TestHistoryJSONRejectsDuplicateNestedFieldsAndTrailingValues(t *testing.T) {
	for _, data := range []string{
		`{"kind":"manifest","kind":"complete"}`,
		`{"input":{"group":0,"group":1}}`,
		`{"watch":[{"revision":2,"revision":3}]}`,
		`{"kind":"manifest"} {"kind":"complete"}`,
		`{"input":null}`,
		`{"pair":["",null]}`,
	} {
		if err := uniqueJSONFields([]byte(data)); err == nil {
			t.Fatalf("ambiguous JSON accepted: %s", data)
		}
	}
	if err := uniqueJSONFields([]byte(`{"operations":[{"input":{"group":0}},{"input":{"group":1}}]}`)); err != nil {
		t.Fatal(err)
	}
}

func TestWatchOracleDoesNotInventAnUnobservedPendingEffectToExplainARead(t *testing.T) {
	prefix := "mock/robustness/42/00000000/"
	kv := watchKV{Key: prefix + "0/register", Value: "mock-b", Revision: 2, CreateRevision: 2, Version: 1}
	record := historyRecord{Prefix: prefix, StartRevision: 1, EndRevision: 2, FinalKV: []watchKV{kv},
		Operations: []historyOperation{
			{Call: 1, Return: 2, Input: historyInput{Kind: "write", Value: "mock-a"}, Output: historyOutput{Uncertain: true}, Error: "mock unavailable"},
			{Call: 3, Return: 4, Input: historyInput{Kind: "read"}, Output: historyOutput{Value: "mock-a"}, Revision: 1},
			{Call: 5, Return: 6, Input: historyInput{Kind: "write", Value: "mock-b"}, Revision: 2},
		}, Watch: []watchEntry{{watchKV: kv}},
	}
	if _, err := checkHistory(record.Operations, time.Second); err != nil {
		t.Fatalf("fixture must demonstrate why the independent watch oracle is necessary: %v", err)
	}
	if err := validateWatch(record); err == nil {
		t.Fatal("a successful read was justified by a pending effect missing from revision-bound watch evidence")
	}
}

func TestWatchOraclePreservesLatePendingEffectsBeyondTheFencedRange(t *testing.T) {
	prefix := "mock/robustness/42/00000000/"
	first := watchKV{Key: prefix + "0/register", Value: "mock-a", Revision: 2, CreateRevision: 2, Version: 1}
	late := watchKV{Key: first.Key, Value: "mock-b", Revision: 3, CreateRevision: 2, Version: 2}
	record := historyRecord{Prefix: prefix, StartRevision: 1, EndRevision: 2, FinalKV: []watchKV{first},
		Operations: []historyOperation{
			{Call: 1, Return: 2, Input: historyInput{Kind: "write", Value: "mock-a"}, Revision: 2},
			{Call: 3, Return: 4, Input: historyInput{Kind: "write", Value: "mock-b"}, Output: historyOutput{Uncertain: true}, Error: "mock unavailable"},
		}, Watch: []watchEntry{{watchKV: first}, {watchKV: late, PrevKV: &first}},
	}
	if err := validateWatch(record); err != nil {
		t.Fatalf("valid late pending effect rejected: %v", err)
	}
}

func TestExternalHistoryRejectsUnexpectedResponsePayloadAndCrossGroupStaleRevisions(t *testing.T) {
	for _, history := range [][]historyOperation{
		{{Call: 1, Return: 2, Input: historyInput{Kind: "write", Value: "mock-a"}, Output: historyOutput{Succeeded: true}, Revision: 2}},
		{{Call: 1, Return: 2, Input: historyInput{Kind: "write", Value: "mock-a"}, Output: historyOutput{Uncertain: true, Value: "mock-partial"}, Error: "mock unavailable"}},
		{{Call: 1, Return: 2, Input: historyInput{Kind: "write", Group: 1, Value: "mock-a"}, Revision: 2}, {Client: 1, Call: 3, Return: 4, Input: historyInput{Kind: "read"}, Revision: 1}},
	} {
		if _, err := checkHistory(history, time.Second); err == nil {
			t.Fatal("invalid response shape or cross-group stale revision accepted")
		}
	}
}
