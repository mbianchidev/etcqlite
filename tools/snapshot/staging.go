package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func stageSnapshot(source, destination string) (result error) {
	if err := verifyChecksum(source); err != nil {
		return err
	}
	input, err := bolt.Open(source, 0o400, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, input.Close()) }()
	file, err := os.CreateTemp(filepath.Dir(destination), ".etcqlite-interchange-*")
	if err != nil {
		return err
	}
	path := file.Name()
	defer func() { result = errors.Join(result, removeStaging(path)) }()
	if err := file.Close(); err != nil {
		return err
	}
	output, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, output.Close()) }()
	if _, err := output.Exec(`PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA trusted_schema=OFF;
		CREATE TABLE interchange_meta(kind TEXT NOT NULL, format INTEGER NOT NULL, reference TEXT NOT NULL, revision INTEGER NOT NULL, compact_revision INTEGER NOT NULL) STRICT;
		CREATE TABLE interchange_buckets(name BLOB PRIMARY KEY) STRICT;
		CREATE TABLE interchange_entries(bucket BLOB NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY(bucket,key)) STRICT;`); err != nil {
		return err
	}
	tx, err := output.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	buckets, err := tx.Prepare("INSERT INTO interchange_buckets VALUES(?)")
	if err != nil {
		return err
	}
	defer buckets.Close()
	entries, err := tx.Prepare("INSERT INTO interchange_entries VALUES(?,?,?)")
	if err != nil {
		return err
	}
	defer entries.Close()
	revision, compact := int64(1), int64(-1)
	err = input.View(func(read *bolt.Tx) error {
		if err := checkSnapshot(read); err != nil {
			return err
		}
		return read.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			if !knownBucket(name) {
				return fmt.Errorf("unsupported source bucket %q", name)
			}
			if _, err := buckets.Exec(name); err != nil {
				return err
			}
			return bucket.ForEach(func(key, value []byte) error {
				if value == nil {
					return errors.New("nested source buckets are unsupported")
				}
				if len(key) == 0 || len(key) > maxRecordBytes || len(value) > maxRecordBytes {
					return errors.New("source key/value exceeds the 16-MiB record limit")
				}
				if bytes.Equal(name, []byte("key")) {
					main, err := decodeRevision(key)
					if err != nil {
						return err
					}
					revision = max(revision, main)
				}
				if bytes.Equal(name, []byte("meta")) {
					if bytes.Equal(key, []byte("storageVersion")) && !bytes.Equal(value, []byte("3.7.0")) && !bytes.Equal(value, []byte("3.6.0")) {
						return errors.New("unsupported source storage version")
					}
					if bytes.Equal(key, []byte("finishedCompactRev")) || bytes.Equal(key, []byte("scheduledCompactRev")) {
						main, err := decodeRevision(value)
						if err != nil {
							return err
						}
						compact, revision = max(compact, main), max(revision, main)
					}
				}
				if value == nil {
					value = []byte{}
				}
				_, err := entries.Exec(name, key, value)
				return err
			})
		})
	})
	if err != nil {
		return err
	}
	for _, name := range []string{"key", "meta"} {
		var count int
		if err := tx.QueryRow("SELECT count(*) FROM interchange_buckets WHERE name=?", []byte(name)).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("source lacks required bucket %q", name)
		}
	}
	if _, err := tx.Exec("INSERT INTO interchange_meta VALUES('bbolt-interchange',2,'3.7.3',?,?)", revision, compact); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	if err := appendChecksum(path); err != nil {
		return err
	}
	if err := verifyChecksum(path); err != nil {
		return err
	}
	return publish(path, destination)
}

func digest(parts ...[]byte) []byte {
	hash := sha256.New()
	for _, part := range parts {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		hash.Write(length[:])
		hash.Write(part)
	}
	return hash.Sum(nil)
}

func number(value int64) []byte {
	var bytes [8]byte
	binary.BigEndian.PutUint64(bytes[:], uint64(value))
	return bytes[:]
}

func encodedRevision(main, sub int64, deleted bool) []byte {
	key := append([]byte{}, number(main)...)
	key = append(key, '_')
	key = append(key, number(sub)...)
	if deleted {
		key = append(key, 't')
	}
	return key
}

type snapshotWriter struct {
	database    *bolt.DB
	transaction *bolt.Tx
	bytes       int
}

func (writer *snapshotWriter) put(bucket string, key, value []byte) error {
	if len(key) == 0 || len(key) > maxRecordBytes || len(value) > maxRecordBytes {
		return errors.New("native export exceeds the 16-MiB record limit")
	}
	if writer.transaction == nil {
		var err error
		writer.transaction, err = writer.database.Begin(true)
		if err != nil {
			return err
		}
	}
	if err := writer.transaction.Bucket([]byte(bucket)).Put(key, value); err != nil {
		return err
	}
	writer.bytes += len(key) + len(value)
	if writer.bytes >= 16<<20 {
		return writer.commit()
	}
	return nil
}

func (writer *snapshotWriter) commit() error {
	if writer.transaction == nil {
		return nil
	}
	err := writer.transaction.Commit()
	writer.transaction = nil
	writer.bytes = 0
	return err
}

func (writer *snapshotWriter) rollback() error {
	if writer.transaction == nil {
		return nil
	}
	err := writer.transaction.Rollback()
	writer.transaction = nil
	return err
}

func exportNative(source, destination string) (result error) {
	if err := verifyChecksum(source); err != nil {
		return fmt.Errorf("native snapshot digest: %w", err)
	}
	absolute, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	uri := url.URL{Scheme: "file", Path: absolute, RawQuery: "mode=ro&immutable=1"}
	input, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, input.Close()) }()
	if _, err := input.Exec("PRAGMA trusted_schema=OFF; PRAGMA query_only=ON;"); err != nil {
		return err
	}
	var version int
	if err := input.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != 1 && version != 2 {
		return errors.New("unsupported native schema")
	}
	var integrity string
	if err := input.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil {
		return err
	}
	if integrity != "ok" {
		return fmt.Errorf("native SQLite integrity=%q", integrity)
	}
	var revision, compact, applied int64
	var authRevision []byte
	var enabled bool
	var downgrade sql.NullString
	if err := input.QueryRow("SELECT revision,compacted_revision,applied_index,auth_revision,auth_enabled,downgrade_target FROM app_state WHERE id=1 AND schema_version=1 AND codec_version=1").
		Scan(&revision, &compact, &applied, &authRevision, &enabled, &downgrade); err != nil {
		return err
	}
	if revision < 1 || compact < -1 || compact > revision || len(authRevision) != 8 {
		return errors.New("native revision/auth metadata is invalid")
	}
	profile := "3.7.0"
	if downgrade.Valid {
		if downgrade.String != "3.6.0" {
			return errors.New("unsupported native tooling profile")
		}
		profile = downgrade.String
	}
	file, err := os.CreateTemp(filepath.Dir(destination), ".etcqlite-native-export-*")
	if err != nil {
		return err
	}
	path := file.Name()
	defer func() { result = errors.Join(result, removeStaging(path)) }()
	if err := file.Close(); err != nil {
		return err
	}
	output, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	writer := &snapshotWriter{database: output}
	defer func() { result = errors.Join(result, writer.rollback()) }()
	err = func() error {
		if err := output.Update(func(tx *bolt.Tx) error {
			for _, name := range []string{"alarm", "auth", "authRoles", "authUsers", "cluster", "key", "lease", "members", "members_removed", "meta"} {
				if _, err := tx.CreateBucket([]byte(name)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		put := writer.put
		flag := byte(0)
		if enabled {
			flag = 1
		}
		for _, item := range []struct {
			bucket, key string
			value       []byte
		}{
			{"auth", "authEnabled", []byte{flag}}, {"auth", "authRevision", authRevision},
			{"cluster", "clusterVersion", []byte(profile)}, {"meta", "storageVersion", []byte(profile)},
			{"meta", "consistent_index", number(applied)}, {"meta", "term", number(1)},
		} {
			if err := put(item.bucket, []byte(item.key), item.value); err != nil {
				return err
			}
		}
		if compact >= 0 {
			for _, key := range []string{"finishedCompactRev", "scheduledCompactRev"} {
				if err := put("meta", []byte(key), encodedRevision(compact, 0, false)); err != nil {
					return err
				}
			}
		}
		for _, table := range []struct{ table, column, bucket string }{{"auth_roles", "role", "authRoles"}, {"auth_users", "user", "authUsers"}} {
			rows, err := input.Query("SELECT name," + table.column + ",checksum FROM " + table.table + " ORDER BY name")
			if err != nil {
				return err
			}
			for rows.Next() {
				var name string
				var value, checksum []byte
				if err := rows.Scan(&name, &value, &checksum); err != nil {
					rows.Close()
					return err
				}
				if !bytes.Equal(digest([]byte(name), value), checksum) {
					rows.Close()
					return errors.New("native auth record checksum mismatch")
				}
				switch table.bucket {
				case "authRoles":
					var role authpb.Role
					if err := proto.Unmarshal(value, &role); err != nil || !bytes.Equal(role.Name, []byte(name)) {
						rows.Close()
						return errors.New("native role encoding/identity mismatch")
					}
				case "authUsers":
					var user authpb.User
					if err := proto.Unmarshal(value, &user); err != nil || !bytes.Equal(user.Name, []byte(name)) {
						rows.Close()
						return errors.New("native user encoding/identity mismatch")
					}
				}
				if err := put(table.bucket, []byte(name), value); err != nil {
					rows.Close()
					return err
				}
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				return err
			}
		}
		rows, err := input.Query("SELECT main_revision,sub_revision,key,kind,kv,prev_kv,checksum FROM mvcc_versions ORDER BY main_revision,sub_revision")
		if err != nil {
			return err
		}
		for rows.Next() {
			var main, sub int64
			var kind int32
			var key, value, previous, checksum []byte
			if err := rows.Scan(&main, &sub, &key, &kind, &value, &previous, &checksum); err != nil {
				rows.Close()
				return err
			}
			var encodedKind [4]byte
			binary.BigEndian.PutUint32(encodedKind[:], uint32(kind))
			if !bytes.Equal(digest(number(main), number(sub), key, encodedKind[:], value, previous), checksum) || main < 1 || main > revision || sub < 0 || (kind != 0 && kind != 1) {
				rows.Close()
				return errors.New("native history checksum/revision mismatch")
			}
			var kv mvccpb.KeyValue
			if err := proto.Unmarshal(value, &kv); err != nil || len(key) == 0 || !bytes.Equal(kv.Key, key) ||
				(kind == 0 && (kv.ModRevision != main || kv.CreateRevision < 1 || kv.CreateRevision > main || kv.Version < 1)) {
				rows.Close()
				return errors.New("native KV encoding/identity mismatch")
			}
			if err := put("key", encodedRevision(main, sub, kind == 1), value); err != nil {
				rows.Close()
				return err
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		rows, err = input.Query("SELECT id,ttl,checkpoint_ttl FROM leases ORDER BY id")
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, ttl, remaining int64
			if err := rows.Scan(&id, &ttl, &remaining); err != nil {
				rows.Close()
				return err
			}
			if id == 0 || ttl < 1 || remaining < 0 || remaining > ttl {
				rows.Close()
				return errors.New("native lease identity/TTL is invalid")
			}
			value, err := proto.Marshal(&leasepb.Lease{ID: id, TTL: ttl, RemainingTTL: remaining})
			if err != nil {
				rows.Close()
				return err
			}
			if err := put("lease", number(id), value); err != nil {
				rows.Close()
				return err
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		rows, err = input.Query("SELECT id,member,checksum FROM cluster_members ORDER BY id")
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, value, checksum []byte
			if err := rows.Scan(&id, &value, &checksum); err != nil {
				rows.Close()
				return err
			}
			if len(id) != 8 || !bytes.Equal(digest(id, value), checksum) {
				rows.Close()
				return errors.New("native member checksum mismatch")
			}
			var member pb.Member
			if err := proto.Unmarshal(value, &member); err != nil {
				rows.Close()
				return err
			}
			if member.ID == 0 || member.ID != binary.BigEndian.Uint64(id) {
				rows.Close()
				return errors.New("native member identity mismatch")
			}
			if member.PeerURLs == nil {
				member.PeerURLs = []string{}
			}
			if member.ClientURLs == nil {
				member.ClientURLs = []string{}
			}
			wire := struct {
				ID         string   `json:"id"`
				PeerURLs   []string `json:"peerURLs"`
				IsLearner  bool     `json:"isLearner,omitempty"`
				Name       string   `json:"name"`
				ClientURLs []string `json:"clientURLs"`
			}{fmt.Sprintf("%x", member.ID), member.PeerURLs, member.IsLearner, member.Name, member.ClientURLs}
			value, err := json.Marshal(wire)
			if err != nil {
				rows.Close()
				return err
			}
			if err := put("members", []byte(wire.ID), value); err != nil {
				rows.Close()
				return err
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		rows, err = input.Query("SELECT id FROM removed_members ORDER BY id")
		if err != nil {
			return err
		}
		for rows.Next() {
			var id []byte
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			if len(id) != 8 {
				rows.Close()
				return errors.New("native removed-member ID width is invalid")
			}
			if err := put("members_removed", []byte(fmt.Sprintf("%x", binary.BigEndian.Uint64(id))), []byte("removed")); err != nil {
				rows.Close()
				return err
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		rows, err = input.Query("SELECT member_id,alarm FROM alarms ORDER BY member_id,alarm")
		if err != nil {
			return err
		}
		for rows.Next() {
			var id []byte
			var alarm int32
			if err := rows.Scan(&id, &alarm); err != nil {
				rows.Close()
				return err
			}
			if len(id) != 8 {
				rows.Close()
				return errors.New("native alarm ID width is invalid")
			}
			value, err := proto.Marshal(&pb.AlarmMember{MemberID: binary.BigEndian.Uint64(id), Alarm: pb.AlarmType(alarm)})
			if err != nil {
				rows.Close()
				return err
			}
			if err := put("alarm", value, []byte{}); err != nil {
				rows.Close()
				return err
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		if err := writer.commit(); err != nil {
			return err
		}
		return output.View(checkSnapshot)
	}()
	if err != nil {
		err = errors.Join(err, writer.rollback())
	}
	if err = errors.Join(err, output.Close()); err != nil {
		return err
	}
	if err := appendChecksum(path); err != nil {
		return err
	}
	if err := verifyChecksum(path); err != nil {
		return err
	}
	return publish(path, destination)
}
