package conformance

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestOfficialClientSyntheticKVWatchAndTxn(t *testing.T) {
	client := startReference(t, "etcqlite")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	put, err := client.Put(ctx, "mock/client/\x00\xff", "\x00\xff\x80")
	if err != nil {
		t.Fatal(err)
	}
	watch := client.Watch(ctx, "mock/client/", clientv3.WithPrefix(), clientv3.WithRev(put.Header.Revision), clientv3.WithPrevKV())
	select {
	case response := <-watch:
		if response.Err() != nil || len(response.Events) != 1 || string(response.Events[0].Kv.Value) != "\x00\xff\x80" {
			t.Fatalf("official watch replay: %v", response)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	response, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("mock/client/\x00\xff"), "=", 1)).
		Then(clientv3.OpPut("mock/client/\x00\xff", "updated"), clientv3.OpPut("mock/client/other", "other")).
		Commit()
	if err != nil || !response.Succeeded || response.Header.Revision != put.Header.Revision+1 {
		t.Fatalf("official transaction: %v, %v", response, err)
	}
	select {
	case event := <-watch:
		if event.Err() != nil || len(event.Events) != 2 || event.Header.Revision != response.Header.Revision {
			t.Fatalf("official watch transaction atomicity: %v", event)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	history, err := client.Get(ctx, "mock/client/\x00\xff", clientv3.WithRev(put.Header.Revision))
	if err != nil || string(history.Kvs[0].Value) != "\x00\xff\x80" {
		t.Fatalf("official historical range: %v, %v", history, err)
	}
}

func TestOfficialConcurrencySessionsMutexAndElection(t *testing.T) {
	client := startReference(t, "etcqlite")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	first, err := concurrency.NewSession(client, concurrency.WithTTL(5), concurrency.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := first.Close(); err != nil {
			t.Errorf("close first synthetic session: %v", err)
		}
	}()
	second, err := concurrency.NewSession(client, concurrency.WithTTL(5), concurrency.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := second.Close(); err != nil {
			t.Errorf("close second synthetic session: %v", err)
		}
	}()
	mutex1, mutex2 := concurrency.NewMutex(first, "mock/official-mutex"), concurrency.NewMutex(second, "mock/official-mutex")
	if err := mutex1.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	acquired := make(chan error, 1)
	go func() { acquired <- mutex2.Lock(ctx) }()
	select {
	case err := <-acquired:
		t.Fatalf("second owner bypassed lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := mutex1.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := mutex2.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	election := concurrency.NewElection(first, "mock/official-election")
	if err := election.Campaign(ctx, "synthetic-leader"); err != nil {
		t.Fatal(err)
	}
	leader, err := election.Leader(ctx)
	if err != nil || string(leader.Kvs[0].Value) != "synthetic-leader" {
		t.Fatalf("official election leader: %v, %v", leader, err)
	}
	if err := election.Proclaim(ctx, "synthetic-update"); err != nil {
		t.Fatal(err)
	}
	if err := election.Resign(ctx); err != nil {
		t.Fatal(err)
	}
}
