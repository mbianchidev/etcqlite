package conformance

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func startReference(t *testing.T, kind string) *clientv3.Client {
	t.Helper()
	variable := "ETCQLITE_" + strings.ToUpper(kind)
	binary := os.Getenv(variable)
	if binary == "" {
		t.Skipf("set %s to the pinned %s reference executable", variable, kind)
	}
	directory := t.TempDir()
	address := freeAddress(t)
	endpoint := "http://" + address
	var args []string
	switch kind {
	case "etcd":
		version, err := exec.Command(binary, "--version").Output()
		if err != nil || !strings.Contains(string(version), "etcd Version: 3.7.3") {
			t.Fatalf("reference must be etcd v3.7.3: %v", err)
		}
		peer := "http://" + freeAddress(t)
		args = []string{
			"--name", "mock-reference",
			"--data-dir", filepath.Join(directory, "node"),
			"--listen-client-urls", endpoint,
			"--advertise-client-urls", endpoint,
			"--listen-peer-urls", peer,
			"--initial-advertise-peer-urls", peer,
			"--initial-cluster", "mock-reference=" + peer,
			"--initial-cluster-token", "synthetic-isolated-reference",
			"--log-level", "error",
		}
	case "kine":
		build, err := exec.Command("go", "version", "-m", binary).Output()
		if err != nil || !strings.Contains(string(build), "github.com/k3s-io/kine\tv0.17.5") {
			t.Fatalf("reference must be Kine v0.17.5: %v", err)
		}
		args = []string{
			"--listen-address", address,
			"--endpoint", "sqlite://" + filepath.Join(directory, "mock.db"),
			"--metrics-bind-address", "0",
			"--log-format", "json",
		}
	default:
		t.Fatalf("unknown reference kind %q", kind)
	}
	logPath := filepath.Join(directory, "server.log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = directory
	for _, value := range os.Environ() {
		name := strings.SplitN(value, "=", 2)[0]
		if !strings.HasPrefix(name, "KINE_") && !strings.HasPrefix(name, "AWS_") {
			command.Env = append(command.Env, value)
		}
	}
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(errors.Join(err, log.Close()))
	}
	done := make(chan struct{})
	var processError error
	go func() {
		processError = command.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		defer cancel()
		select {
		case <-done:
		default:
			if err := command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("interrupt owned reference process: %v", err)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					t.Errorf("stop owned reference process: %v", err)
				}
				<-done
			}
		}
		if err := log.Close(); err != nil {
			t.Errorf("close owned reference log: %v", err)
		}
		if t.Failed() {
			content, err := os.ReadFile(logPath)
			if err != nil {
				t.Errorf("read reference diagnostics: %v", err)
			} else {
				t.Logf("reference diagnostics:\n%s", content)
			}
		}
	})
	if _, err := exec.LookPath("lsof"); err != nil {
		t.Fatal("lsof is required to verify the reference owns its listener before issuing requests")
	}
	deadline := time.Now().Add(15 * time.Second)
	port := strings.Split(address, ":")[1]
	for {
		probe := exec.Command(
			"lsof", "-nP", "-a", "-p", strconv.Itoa(command.Process.Pid),
			"-iTCP:"+port, "-sTCP:LISTEN",
		)
		if probe.Run() == nil {
			break
		}
		select {
		case <-done:
			t.Fatalf("reference exited before binding its owned listener: %v", processError)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("reference did not bind its owned listener before startup deadline")
		}
		time.Sleep(50 * time.Millisecond)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithNoProxy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close reference client: %v", err)
		}
	})
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, err := client.Get(ctx, "mock/startup-probe")
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reference never became ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return client
}

func TestEtcdReferenceFacts(t *testing.T) {
	client := startReference(t, "etcd")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	rangeResponse, err := client.Get(ctx, "mock/not-present")
	if err != nil {
		t.Fatal(err)
	}
	if rangeResponse.Header.Revision != 1 || rangeResponse.Count != 0 {
		t.Fatalf("unexpected initial MVCC state: %v", rangeResponse)
	}
	auth, err := client.AuthStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if auth.Enabled || auth.AuthRevision != 1 {
		t.Fatalf("unexpected initial auth state: %v", auth)
	}
	lease, err := client.Grant(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("default reference minimum lease TTL: %d", lease.TTL)
	if lease.TTL != 2 || lease.ResponseHeader.Revision != 1 {
		t.Fatalf("grant changed the KV revision or shortened TTL: %v", lease)
	}
	put, err := client.Put(ctx, "mock/a", "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	txn, err := client.Txn(ctx).Then(
		clientv3.OpGet("mock/a"),
		clientv3.OpPut("mock/b", "synthetic"),
		clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpGet("mock/b")}, nil),
	).Commit()
	if err != nil {
		t.Fatal(err)
	}
	if txn.Header.Revision != put.Header.Revision+1 {
		t.Fatal("modifying transaction did not allocate exactly one revision")
	}
	t.Logf("txn final/before-put/after-put headers: %d/%d/%d",
		txn.Header.Revision,
		txn.Responses[0].GetResponseRange().Header.Revision,
		txn.Responses[2].GetResponseTxn().Responses[0].GetResponseRange().Header.Revision,
	)
	if txn.Responses[0].GetResponseRange().Header.Revision != 2 ||
		txn.Responses[2].GetResponseTxn().Responses[0].GetResponseRange().Header.Revision != 3 ||
		txn.Responses[2].GetResponseTxn().Header.Revision != 0 {
		t.Fatal("nested transaction header timing differs from the pinned reference")
	}
	raw := pb.NewKVClient(client.ActiveConnection())
	_, err = raw.Txn(ctx, &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("mock/same")}}},
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("mock/same")}}},
	}})
	if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != "etcdserver: duplicate key given in txn request" {
		t.Fatalf("duplicate-key identity differs: %v", err)
	}
	history, err := client.Get(ctx, "mock/a", clientv3.WithRev(put.Header.Revision))
	if err != nil {
		t.Fatal(err)
	}
	if history.Header.Revision != txn.Header.Revision {
		t.Fatalf("historical response header did not report the snapshot's current frontier: %v", history)
	}
	ttl, err := client.TimeToLive(ctx, 123456, clientv3.WithAttachedKeys())
	if err != nil {
		t.Fatal(err)
	}
	if ttl.TTL != -1 || ttl.GrantedTTL != 0 {
		t.Fatalf("unknown lease sentinel differs: %v", ttl)
	}
	_, err = client.AuthEnable(ctx)
	if !errors.Is(err, rpctypes.ErrRootUserNotExist) {
		t.Fatalf("auth root precondition differs: %v", err)
	}
}

func TestKineLocalBaseline(t *testing.T) {
	client := startReference(t, "kine")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	response, err := client.Put(ctx, "mock/binary/\x00\xff", "\x00\xff\x80")
	if err != nil {
		t.Fatal(err)
	}
	value, err := client.Get(ctx, "mock/binary/\x00\xff")
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Kvs) != 1 || string(value.Kvs[0].Value) != "\x00\xff\x80" {
		t.Fatal("Kine SQLite baseline altered binary data")
	}
	if value.Header.Revision < response.Header.Revision {
		t.Fatal("baseline current read predates its acknowledged write")
	}
	capabilities := make(map[string]string)
	_, err = client.AuthStatus(ctx)
	capabilities["AuthStatus"] = status.Code(err).String()
	stream, err := pb.NewKVClient(client.ActiveConnection()).RangeStream(ctx, &pb.RangeRequest{
		Key: []byte("mock/binary/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("mock/binary/")),
	})
	if err == nil {
		_, err = stream.Recv()
	}
	capabilities["RangeStream"] = status.Code(err).String()
	_, err = client.HashKV(ctx, client.Endpoints()[0], 0)
	capabilities["HashKV"] = status.Code(err).String()
	t.Logf("Kine v0.17.5 SQLite baseline capabilities: %v", capabilities)
}

func TestReferenceFilterAndSortCounts(t *testing.T) {
	client := startReference(t, "etcd")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	for index, value := range []string{"z", "y", "a"} {
		if _, err := client.Put(ctx, fmt.Sprintf("mock/sort/%02d", index), value); err != nil {
			t.Fatal(err)
		}
	}
	raw := pb.NewKVClient(client.ActiveConnection())
	response, err := raw.Range(ctx, &pb.RangeRequest{
		Key: []byte("mock/sort/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("mock/sort/")),
		Limit: 1, SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_NONE,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("NONE/value/limit=1: count=%d, key=%q, more=%v", response.Count, response.Kvs[0].Key, response.More)
	if response.Count != 3 || !response.More || string(response.Kvs[0].Key) != "mock/sort/01" {
		t.Fatal("reference pre-limit sort behavior changed")
	}
	filtered, err := raw.Range(ctx, &pb.RangeRequest{
		Key: []byte("mock/sort/"), RangeEnd: []byte(clientv3.GetPrefixRangeEnd("mock/sort/")),
		MinModRevision: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("revision-filter result: count=%d, returned=%d", filtered.Count, len(filtered.Kvs))
	if filtered.Count != 3 || len(filtered.Kvs) != 1 || filtered.Kvs[0].ModRevision != 4 {
		t.Fatal("reference revision filter did not isolate the synthetic last mutation")
	}
}
