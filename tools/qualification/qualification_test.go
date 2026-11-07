package main

import (
	"testing"
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
