package nodebase

// Pack-local unit tests for TLSConfig.Build — the tlsConfig node's
// material loader — covering flag mapping and the CA/cert error paths
// without needing a live TLS peer.

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTLSConfigBuildFlags(t *testing.T) {
	cfg, err := (&TLSConfig{VerifyServerCert: false, ServerName: "example.test"}).Build()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("verifyservercert=false should set InsecureSkipVerify")
	}
	if cfg.ServerName != "example.test" {
		t.Fatal("servername not propagated")
	}
	cfg, err = (&TLSConfig{VerifyServerCert: true}).Build()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("verifyservercert=true must verify")
	}
}

func TestTLSConfigBuildCA(t *testing.T) {
	dir := t.TempDir()
	// bad path
	if _, err := (&TLSConfig{CAPath: filepath.Join(dir, "missing.pem")}).Build(); err == nil {
		t.Fatal("missing CA file should error")
	}
	// file with no PEM certs
	bad := filepath.Join(dir, "bad.pem")
	_ = os.WriteFile(bad, []byte("not a pem"), 0644)
	if _, err := (&TLSConfig{CAPath: bad}).Build(); err == nil {
		t.Fatal("garbage CA should error")
	}
	// valid self-signed CA
	certPEM, keyPEM := selfSignedPEM(t)
	ca := filepath.Join(dir, "ca.pem")
	_ = os.WriteFile(ca, certPEM, 0644)
	cfg, err := (&TLSConfig{CAPath: ca, VerifyServerCert: true}).Build()
	if err != nil {
		t.Fatalf("valid CA rejected: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs not populated")
	}
	// cert+key pair loads into Certificates
	cert := filepath.Join(dir, "cert.pem")
	key := filepath.Join(dir, "key.pem")
	_ = os.WriteFile(cert, certPEM, 0644)
	_ = os.WriteFile(key, keyPEM, 0644)
	cfg, err = (&TLSConfig{CertPath: cert, KeyPath: key}).Build()
	if err != nil {
		t.Fatalf("cert/key load failed: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatal("certificate pair not loaded")
	}
	// mismatched pair errors
	_ = os.WriteFile(key, []byte("garbage"), 0644)
	if _, err := (&TLSConfig{CertPath: cert, KeyPath: key}).Build(); err == nil {
		t.Fatal("bad key should error")
	}
}

// selfSignedPEM mints a throwaway self-signed cert/key pair in PEM.
func selfSignedPEM(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("keygen failed: %v", err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert create failed: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
