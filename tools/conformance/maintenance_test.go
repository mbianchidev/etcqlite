package conformance

import (
	"context"
	"reflect"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestHashKVHistoryAndRepeatedCompactionMatchPinnedEtcd(t *testing.T) {
	type hash struct {
		Value    uint32
		Revision int64
		Compact  int64
	}
	run := func(kind string) []hash {
		t.Helper()
		client := startReference(t, kind)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		var hashes []hash
		check := func(revision int64) {
			t.Helper()
			result, err := client.HashKV(ctx, client.Endpoints()[0], revision)
			if err != nil {
				t.Fatal(err)
			}
			hashes = append(hashes, hash{result.Hash, result.HashRevision, result.CompactRevision})
		}
		check(0)
		for _, value := range []string{"\x00\xff", "\xff\x00"} {
			if _, err := client.Put(ctx, "mock/hash/\x00\xff", value); err != nil {
				t.Fatal(err)
			}
		}
		check(2)
		check(0)
		if _, err := client.Compact(ctx, 2, clientv3.WithCompactPhysical()); err != nil {
			t.Fatal(err)
		}
		check(2)
		check(0)
		if _, err := client.Delete(ctx, "mock/hash/\x00\xff"); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Put(ctx, "mock/hash/other", "synthetic"); err != nil {
			t.Fatal(err)
		}
		check(0)
		if _, err := client.Compact(ctx, 5, clientv3.WithCompactPhysical()); err != nil {
			t.Fatal(err)
		}
		check(5)
		check(0)
		return hashes
	}
	reference, candidate := run("etcd"), run("etcqlite")
	if !reflect.DeepEqual(reference, candidate) {
		t.Fatalf("reference hashes %v differ from candidate %v", reference, candidate)
	}
}
