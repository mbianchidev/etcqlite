package main

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestDiskBackedInterchangePreservesEveryBucketRecord(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.db")
	staged := filepath.Join(directory, "interchange.sqlite")
	original := fixture(t)
	if err := exportSnapshot(original, source); err != nil {
		t.Fatal(err)
	}
	if err := stageSnapshot(source, staged); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+staged+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, bucket := range original.Buckets {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM interchange_entries WHERE bucket = ?", bucket.Name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != len(bucket.Entries) {
			t.Fatal("streaming interchange lost records")
		}
		for _, entry := range bucket.Entries {
			var value []byte
			if err := db.QueryRow("SELECT value FROM interchange_entries WHERE bucket = ? AND key = ?", bucket.Name, entry.Key).Scan(&value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(value, entry.Value) {
				t.Fatal("streaming interchange changed binary data")
			}
		}
	}
}
