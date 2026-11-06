package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func marshal(t *testing.T, value proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func revisionKey(main, sub uint64, deleted bool) []byte {
	key := make([]byte, 17)
	binary.BigEndian.PutUint64(key, main)
	key[8] = '_'
	binary.BigEndian.PutUint64(key[9:], sub)
	if deleted {
		key = append(key, 't')
	}
	return key
}

func fixture(t *testing.T) Manifest {
	t.Helper()
	key := []byte{0, 255, 128, 1}
	lease := make([]byte, 8)
	binary.BigEndian.PutUint64(lease, 7)
	var leaseValue []byte
	for field, value := range []uint64{7, 60, 30} {
		leaseValue = protowire.AppendTag(leaseValue, protowire.Number(field+1), protowire.VarintType)
		leaseValue = protowire.AppendVarint(leaseValue, value)
	}
	authRevision := make([]byte, 8)
	binary.BigEndian.PutUint64(authRevision, 7)
	return Manifest{
		Format:          1,
		Reference:       "3.7.3",
		Revision:        4,
		CompactRevision: -1,
		Buckets: []Bucket{
			{Name: []byte("alarm")},
			{Name: []byte("auth"), Entries: []BucketEntry{
				{Key: []byte("authEnabled"), Value: []byte{0}},
				{Key: []byte("authRevision"), Value: authRevision},
			}},
			{Name: []byte("authRoles"), Entries: []BucketEntry{
				{Key: []byte("mock-role"), Value: marshal(t, &authpb.Role{
					Name: []byte("mock-role"),
					KeyPermission: []*authpb.Permission{{
						PermType: authpb.Permission_READWRITE,
						Key:      key,
						RangeEnd: []byte{0},
					}},
				})},
			}},
			{Name: []byte("authUsers"), Entries: []BucketEntry{
				{Key: []byte("mock-user"), Value: marshal(t, &authpb.User{
					Name:     []byte("mock-user"),
					Password: []byte("synthetic-hash-not-a-customer-credential"),
					Roles:    []string{"mock-role"},
				})},
			}},
			{Name: []byte("cluster"), Entries: []BucketEntry{
				{Key: []byte("clusterVersion"), Value: []byte("3.7.0")},
			}},
			{Name: []byte("key"), Entries: []BucketEntry{
				{Key: revisionKey(2, 0, false), Value: marshal(t, &mvccpb.KeyValue{
					Key: key, Value: []byte{0, 255, 128}, CreateRevision: 2,
					ModRevision: 2, Version: 1, Lease: 7,
				})},
				{Key: revisionKey(3, 0, true), Value: marshal(t, &mvccpb.KeyValue{Key: key})},
				{Key: revisionKey(4, 0, false), Value: marshal(t, &mvccpb.KeyValue{
					Key: key, Value: []byte{3}, CreateRevision: 4,
					ModRevision: 4, Version: 1, Lease: 7,
				})},
			}},
			{Name: []byte("lease"), Entries: []BucketEntry{{Key: lease, Value: leaseValue}}},
			{Name: []byte("members")},
			{Name: []byte("members_removed")},
			{Name: []byte("meta"), Entries: []BucketEntry{
				{Key: []byte("consistent_index"), Value: []byte{0, 0, 0, 0, 0, 0, 0, 1}},
				{Key: []byte("storageVersion"), Value: []byte("3.7.0")},
				{Key: []byte("term"), Value: []byte{0, 0, 0, 0, 0, 0, 0, 1}},
			}},
		},
	}
}

func TestSnapshotPreservesBinaryHistoryAuthLeasesAndMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	original := fixture(t)
	if err := exportSnapshot(original, path); err != nil {
		t.Fatal(err)
	}
	recovered, err := importSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered, original) {
		t.Fatalf("snapshot round-trip changed application state:\n%#v\n%#v", original, recovered)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("snapshot is not private: %o", info.Mode().Perm())
	}
}

func TestSnapshotFailureNeverReplacesExistingArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	previous := []byte("previous verified artifact")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := exportSnapshot(fixture(t), path); err == nil {
		t.Fatal("snapshot publication overwrote an existing artifact")
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(previous, actual) {
		t.Fatal("failed publication changed the previous artifact")
	}
}

func TestSnapshotChecksumRejectsTamperedAndTruncatedData(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "snapshot.db")
	if err := exportSnapshot(fixture(t), path); err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bytes[9000] ^= 1
	corrupt := filepath.Join(directory, "corrupt.db")
	if err := os.WriteFile(corrupt, bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := importSnapshot(corrupt); err == nil {
		t.Fatal("tampered snapshot passed validation")
	}
	truncated := filepath.Join(directory, "truncated.db")
	if err := os.WriteFile(truncated, bytes[:len(bytes)-1], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := importSnapshot(truncated); err == nil {
		t.Fatal("truncated snapshot passed validation")
	}
}

func TestManifestRejectsIncompatibleVersionsAndDuplicateRecords(t *testing.T) {
	for _, mutate := range []func(*Manifest){
		func(m *Manifest) { m.Format = 2 },
		func(m *Manifest) { m.Reference = "unsupported" },
		func(m *Manifest) { m.Buckets = append(m.Buckets, m.Buckets[0]) },
		func(m *Manifest) {
			m.Buckets[1].Entries = append(m.Buckets[1].Entries, m.Buckets[1].Entries[0])
		},
	} {
		manifest := fixture(t)
		mutate(&manifest)
		if err := exportSnapshot(manifest, filepath.Join(t.TempDir(), "snapshot.db")); err == nil {
			t.Fatal("invalid manifest was accepted")
		}
	}
}

func TestSnapshotWorksWithPinnedEtcdutlStatusAndRestore(t *testing.T) {
	tool := os.Getenv("ETCQLITE_ETCDUTL")
	if tool == "" {
		t.Skip("set ETCQLITE_ETCDUTL to the pinned etcdutl v3.7.3 executable")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "snapshot.db")
	if err := exportSnapshot(fixture(t), path); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(tool, "snapshot", "status", path, "--write-out=json").CombinedOutput()
	if err != nil {
		t.Fatalf("etcdutl snapshot status: %v\n%s", err, output)
	}
	var status struct {
		Revision int64  `json:"revision"`
		TotalKey int    `json:"totalKey"`
		Version  string `json:"version"`
	}
	if err := json.Unmarshal(output, &status); err != nil {
		t.Fatalf("invalid etcdutl status: %v\n%s", err, output)
	}
	if status.Revision != 4 || status.TotalKey != 1 || status.Version != "3.7.0" {
		t.Fatalf("wrong etcdutl status: %+v", status)
	}
	output, err = exec.Command(
		tool, "snapshot", "restore", path, "--data-dir", filepath.Join(directory, "restored"),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("etcdutl snapshot restore: %v\n%s", err, output)
	}
}
