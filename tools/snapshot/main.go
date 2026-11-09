package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

const maxSnapshotBytes int64 = 16 << 30
const maxManifestBytes int64 = 512 << 20
const maxRecordBytes = 16 << 20

var version = "development"
var buildSHA = "unknown"
var buildDirty = "true"

func knownBucket(name []byte) bool {
	switch string(name) {
	case "alarm", "auth", "authRoles", "authUsers", "cluster", "key", "lease", "members", "members_removed", "meta":
		return true
	default:
		return false
	}
}

type Manifest struct {
	Format          uint32   `json:"format"`
	Reference       string   `json:"reference"`
	Revision        int64    `json:"revision"`
	CompactRevision int64    `json:"compact_revision"`
	Buckets         []Bucket `json:"buckets"`
}

type Bucket struct {
	Name    []byte        `json:"name"`
	Entries []BucketEntry `json:"entries"`
}

type BucketEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func main() {
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case "--version":
			fmt.Printf("etcqlite-snapshot %s\n", version)
			return
		case "--build-info":
			if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
				"version": version, "build": buildSHA, "dirty": buildDirty == "true",
			}); err != nil {
				fmt.Fprintln(os.Stderr, "etcqlite-snapshot build info:", err)
				os.Exit(1)
			}
			return
		case "--help", "-h":
			fmt.Println("Usage: etcqlite-snapshot export <manifest.json> <snapshot.db> | import <snapshot.db> <manifest.json> | stage <snapshot.db> <interchange.sqlite> | export-native <native.snapshot> <snapshot.db>")
			fmt.Println("Options: --version, --build-info, --help")
			return
		}
	}
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "etcqlite-snapshot:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 3 {
		return errors.New("usage: etcqlite-snapshot export <manifest.json> <snapshot.db> | import <snapshot.db> <manifest.json> | stage <snapshot.db> <interchange.sqlite> | export-native <native.snapshot> <snapshot.db>")
	}
	switch args[0] {
	case "stage":
		return stageSnapshot(args[1], args[2])
	case "export-native":
		return exportNative(args[1], args[2])
	case "export":
		manifest, err := readManifest(args[1])
		if err != nil {
			return err
		}
		return exportSnapshot(manifest, args[2])
	case "import":
		manifest, err := importSnapshot(args[1])
		if err != nil {
			return err
		}
		return writeManifest(manifest, args[2])
	default:
		return fmt.Errorf("unknown snapshot operation %q", args[0])
	}
}

func readManifest(path string) (manifest Manifest, result error) {
	file, err := os.Open(path)
	if err != nil {
		return manifest, fmt.Errorf("open manifest: %w", err)
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return manifest, err
	}
	if info.Size() > maxManifestBytes {
		return manifest, fmt.Errorf("manifest exceeds the %d-byte JSON converter limit; use disk-backed stage/native conversion", maxManifestBytes)
	}
	decoder := json.NewDecoder(io.LimitReader(file, maxManifestBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, fmt.Errorf("decode manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return manifest, errors.New("manifest contains trailing data")
	}
	return manifest, validateManifest(manifest)
}

func validateManifest(manifest Manifest) error {
	if manifest.Format != 1 || manifest.Reference != "3.7.3" {
		return errors.New("unsupported snapshot manifest format or etcd reference")
	}
	if manifest.Revision < 1 || manifest.CompactRevision < -1 || manifest.CompactRevision > manifest.Revision {
		return errors.New("invalid snapshot revision boundaries")
	}
	keyBucket, metaBucket := false, false
	var previousBucket []byte
	for index, bucket := range manifest.Buckets {
		if !knownBucket(bucket.Name) || (index > 0 && bytes.Compare(previousBucket, bucket.Name) >= 0) {
			return errors.New("snapshot bucket names must be known, unique and bytewise sorted")
		}
		previousBucket = bucket.Name
		keyBucket = keyBucket || bytes.Equal(bucket.Name, []byte("key"))
		metaBucket = metaBucket || bytes.Equal(bucket.Name, []byte("meta"))
		var previousKey []byte
		for index, entry := range bucket.Entries {
			if len(entry.Key) == 0 || (index > 0 && bytes.Compare(previousKey, entry.Key) >= 0) {
				return errors.New("snapshot bucket keys must be unique and bytewise sorted")
			}
			previousKey = entry.Key
			if bytes.Equal(bucket.Name, []byte("key")) {
				main, err := decodeRevision(entry.Key)
				if err != nil {
					return err
				}
				if main > manifest.Revision {
					return errors.New("MVCC history exceeds the snapshot revision")
				}
			}
		}
	}
	if !keyBucket || !metaBucket {
		return errors.New("snapshot requires key and meta buckets")
	}
	return nil
}

func decodeRevision(key []byte) (int64, error) {
	if (len(key) != 17 && len(key) != 18) || key[8] != '_' || (len(key) == 18 && key[17] != 't') {
		return 0, errors.New("invalid MVCC revision encoding")
	}
	main, sub := binary.BigEndian.Uint64(key[:8]), binary.BigEndian.Uint64(key[9:17])
	if main > math.MaxInt64 || sub > math.MaxInt64 {
		return 0, errors.New("MVCC revision exceeds signed 64-bit range")
	}
	return int64(main), nil
}

func exportSnapshot(manifest Manifest, path string) (result error) {
	if err := validateManifest(manifest); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".etcqlite-snapshot-*")
	if err != nil {
		return fmt.Errorf("create private snapshot staging file: %w", err)
	}
	name := temporary.Name()
	defer func() { result = errors.Join(result, removeStaging(name)) }()
	if err := temporary.Close(); err != nil {
		return err
	}
	database, err := bolt.Open(name, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("open staged bbolt snapshot: %w", err)
	}
	err = database.Update(func(transaction *bolt.Tx) error {
		for _, bucket := range manifest.Buckets {
			target, err := transaction.CreateBucket(bucket.Name)
			if err != nil {
				return fmt.Errorf("create snapshot bucket: %w", err)
			}
			for _, entry := range bucket.Entries {
				if err := target.Put(entry.Key, entry.Value); err != nil {
					return fmt.Errorf("write snapshot bucket entry: %w", err)
				}
			}
		}
		return nil
	})
	if err == nil {
		err = database.Sync()
	}
	if err = errors.Join(err, database.Close()); err != nil {
		return fmt.Errorf("commit staged snapshot: %w", err)
	}
	if err := appendChecksum(name); err != nil {
		return err
	}
	if _, err := importSnapshot(name); err != nil {
		return fmt.Errorf("verify staged snapshot: %w", err)
	}
	return publish(name, path)
}

func appendChecksum(path string) (result error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("hash staged snapshot: %w", err)
	}
	if _, err := file.Write(hash.Sum(nil)); err != nil {
		return fmt.Errorf("append snapshot checksum: %w", err)
	}
	return file.Sync()
}

func verifyChecksum(path string) (result error) {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open snapshot: %w", err)
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 512+sha256.Size || info.Size()%512 != sha256.Size {
		return errors.New("snapshot is truncated or lacks an upstream SHA-256 trailer")
	}
	if info.Size() > maxSnapshotBytes {
		return fmt.Errorf("snapshot exceeds the %d-byte offline converter limit", maxSnapshotBytes)
	}
	hash := sha256.New()
	if _, err := io.CopyN(hash, file, info.Size()-sha256.Size); err != nil {
		return fmt.Errorf("read snapshot checksum input: %w", err)
	}
	trailer := make([]byte, sha256.Size)
	if _, err := io.ReadFull(file, trailer); err != nil {
		return fmt.Errorf("read snapshot checksum: %w", err)
	}
	if subtle.ConstantTimeCompare(hash.Sum(nil), trailer) != 1 {
		return errors.New("snapshot SHA-256 checksum mismatch")
	}
	return nil
}

func checkSnapshot(transaction *bolt.Tx) error {
	var failures []error
	for err := range transaction.Check() {
		if len(failures) < 10 {
			failures = append(failures, err)
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("bbolt snapshot integrity: %w", errors.Join(failures...))
	}
	return nil
}

func importSnapshot(path string) (manifest Manifest, result error) {
	if err := verifyChecksum(path); err != nil {
		return manifest, err
	}
	database, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return manifest, fmt.Errorf("open read-only snapshot: %w", err)
	}
	defer func() { result = errors.Join(result, database.Close()) }()
	manifest = Manifest{Format: 1, Reference: "3.7.3", Revision: 1, CompactRevision: -1}
	err = database.View(func(transaction *bolt.Tx) error {
		if err := checkSnapshot(transaction); err != nil {
			return err
		}
		return transaction.ForEach(func(name []byte, source *bolt.Bucket) error {
			bucket := Bucket{Name: bytes.Clone(name)}
			err := source.ForEach(func(key, value []byte) error {
				if value == nil {
					return errors.New("nested bbolt buckets are not supported by the pinned etcd schema")
				}
				bucket.Entries = append(bucket.Entries, BucketEntry{
					Key: bytes.Clone(key), Value: bytes.Clone(value),
				})
				if bytes.Equal(name, []byte("key")) {
					main, err := decodeRevision(key)
					if err != nil {
						return err
					}
					manifest.Revision = max(manifest.Revision, main)
				}
				if bytes.Equal(name, []byte("meta")) {
					if bytes.Equal(key, []byte("storageVersion")) && !bytes.Equal(value, []byte("3.7.0")) && !bytes.Equal(value, []byte("3.6.0")) {
						return errors.New("source storage version is not a supported 3.6.0/3.7.0 format")
					}
					if bytes.Equal(key, []byte("finishedCompactRev")) || bytes.Equal(key, []byte("scheduledCompactRev")) {
						compact, err := decodeRevision(value)
						if err != nil {
							return fmt.Errorf("invalid compaction metadata: %w", err)
						}
						manifest.CompactRevision = max(manifest.CompactRevision, compact)
						manifest.Revision = max(manifest.Revision, compact)
					}
				}
				return nil
			})
			manifest.Buckets = append(manifest.Buckets, bucket)
			return err
		})
	})
	if err != nil {
		return manifest, err
	}
	return manifest, validateManifest(manifest)
}

func writeManifest(manifest Manifest, path string) (result error) {
	// Keep empty collections and binary values canonical across Go and Rust.
	manifest.Buckets = append([]Bucket(nil), manifest.Buckets...)
	for index := range manifest.Buckets {
		source := manifest.Buckets[index].Entries
		manifest.Buckets[index].Entries = make([]BucketEntry, len(source))
		copy(manifest.Buckets[index].Entries, source)
		for entry := range manifest.Buckets[index].Entries {
			if manifest.Buckets[index].Entries[entry].Value == nil {
				manifest.Buckets[index].Entries[entry].Value = []byte{}
			}
		}
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".etcqlite-manifest-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer func() { result = errors.Join(result, removeStaging(name)) }()
	err = json.NewEncoder(temporary).Encode(manifest)
	if err == nil {
		err = temporary.Sync()
	}
	if err = errors.Join(err, temporary.Close()); err != nil {
		return fmt.Errorf("write private manifest: %w", err)
	}
	return publish(name, path)
}

func publish(staging, destination string) (result error) {
	// A hard link publishes atomically without a check-then-rename overwrite race.
	if err := os.Link(staging, destination); err != nil {
		return fmt.Errorf("publish without replacing existing data: %w", err)
	}
	directory, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return fmt.Errorf("open publication directory for sync: %w", err)
	}
	defer func() { result = errors.Join(result, directory.Close()) }()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync publication directory: %w", err)
	}
	return nil
}

func removeStaging(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove owned staging file: %w", err)
	}
	return nil
}
