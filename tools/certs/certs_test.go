package main

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestSyntheticCertificatesBindApprovedGuestHosts(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "pki")
	if err := createForHosts(directory, 42, "1,2,3", "127.0.0.1,localhost,lima-etcqlite-qual-3aee-2.internal"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(directory, "member-2.crt"))
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatal("missing synthetic certificate PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"127.0.0.1", "localhost", "lima-etcqlite-qual-3aee-2.internal"} {
		if err := certificate.VerifyHostname(host); err != nil {
			t.Fatal(err)
		}
	}
	if certificate.VerifyHostname("unapproved.invalid") == nil {
		t.Fatal("certificate granted an unapproved host")
	}
}
