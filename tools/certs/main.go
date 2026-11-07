package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	out := flag.String("out", "", "new private output directory for synthetic development certificates")
	cluster := flag.Uint64("cluster-id", 42, "synthetic cluster ID")
	members := flag.String("members", "1,2,3", "comma-separated synthetic member IDs")
	flag.Parse()
	if err := create(*out, *cluster, *members); err != nil {
		fmt.Fprintln(os.Stderr, "development certificate generation:", err)
		os.Exit(1)
	}
}

func create(out string, cluster uint64, members string) error {
	if out == "" || cluster == 0 {
		return fmt.Errorf("--out and a nonzero --cluster-id are required")
	}
	if err := os.Mkdir(out, 0o700); err != nil {
		return fmt.Errorf("create new private output directory: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "Etcqlite synthetic development CA"},
		NotBefore:    now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return err
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		return err
	}
	if err := write(out, "ca.crt", "CERTIFICATE", der); err != nil {
		return err
	}
	for _, value := range strings.Split(members, ",") {
		id, err := strconv.ParseUint(value, 10, 64)
		if err != nil || id == 0 {
			return fmt.Errorf("invalid synthetic member ID")
		}
		memberKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		if err != nil {
			return err
		}
		identity, err := url.Parse(fmt.Sprintf("spiffe://etcqlite/%016x/%016x", cluster, id))
		if err != nil {
			return err
		}
		member := &x509.Certificate{
			SerialNumber: serial, Subject: pkix.Name{CommonName: fmt.Sprintf("mock-member-%d", id)},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
			BasicConstraintsValid: true,
			KeyUsage:              x509.KeyUsageDigitalSignature,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
			URIs: []*url.URL{identity},
		}
		der, err := x509.CreateCertificate(rand.Reader, member, ca, &memberKey.PublicKey, key)
		if err != nil {
			return err
		}
		if err := write(out, fmt.Sprintf("member-%d.crt", id), "CERTIFICATE", der); err != nil {
			return err
		}
		private, err := x509.MarshalPKCS8PrivateKey(memberKey)
		if err != nil {
			return err
		}
		if err := write(out, fmt.Sprintf("member-%d.key", id), "PRIVATE KEY", private); err != nil {
			return err
		}
	}
	return nil
}

func write(directory, name, kind string, data []byte) (result error) {
	path := filepath.Join(directory, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	if err := pem.Encode(file, &pem.Block{Type: kind, Bytes: data}); err != nil {
		return fmt.Errorf("write synthetic certificate: %w", err)
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}
