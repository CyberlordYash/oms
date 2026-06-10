//go:build ignore

// gen.go generates a self-signed CA and per-service TLS certificates for local
// development and Docker mTLS.  Run once from the repo root:
//
//	go run ./certs/gen.go
//
// Output (written to certs/):
//
//	ca.crt              CA certificate  (safe to commit)
//	ca.key              CA private key  (gitignored — keep secret)
//	risk-engine.crt     signed service cert
//	risk-engine.key     service private key (gitignored)
//	order-service.crt   signed service cert
//	order-service.key   service private key (gitignored)
//
// Then uncomment the mTLS env-var blocks in docker-compose.yml and restart.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const outDir = "certs"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
	fmt.Println("certs written to", outDir+"/")
	fmt.Println("NOTE: *.key files are gitignored. Do not commit them.")
}

func run() error {
	// ── CA ────────────────────────────────────────────────────────────────────
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate CA key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "OMS Dev CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("self-sign CA: %w", err)
	}
	caCert, _ := x509.ParseCertificate(caDER)

	if err := writeCert("ca.crt", caDER); err != nil {
		return err
	}
	if err := writeKey("ca.key", caKey); err != nil {
		return err
	}

	// ── Service certs ─────────────────────────────────────────────────────────
	services := []struct {
		name string
		sans []string // DNS SANs; localhost and 127.0.0.1 are always added
	}{
		{"risk-engine", []string{"risk-engine"}},
		{"order-service", []string{"order-service"}},
	}

	for i, svc := range services {
		svcKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return fmt.Errorf("generate key for %s: %w", svc.name, err)
		}

		dnsNames := append([]string{"localhost"}, svc.sans...)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(10 + i)),
			Subject:      pkix.Name{CommonName: svc.name},
			DNSNames:     dnsNames,
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}

		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &svcKey.PublicKey, caKey)
		if err != nil {
			return fmt.Errorf("sign cert for %s: %w", svc.name, err)
		}

		if err := writeCert(svc.name+".crt", der); err != nil {
			return err
		}
		if err := writeKey(svc.name+".key", svcKey); err != nil {
			return err
		}
	}

	return nil
}

func writeCert(name string, der []byte) error {
	f, err := os.Create(filepath.Join(outDir, name))
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writeKey(name string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal key for %s: %w", name, err)
	}
	f, err := os.Create(filepath.Join(outDir, name))
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}
