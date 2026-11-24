package nodebase

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig holds configuration for the tlsConfig node — the Node-RED
// `tls-config` config node equivalent. It is a shared config entity:
// tcp nodes reference it by node id via their "tls" field, and the
// host materializes a *tls.Config from its stored paths at message
// time (NodeHost.LoadTLSConfig). It has no ports and never executes.
type TLSConfig struct {
	// CertPath is the client/server certificate file (PEM).
	CertPath string `json:"cert"`
	// KeyPath is the private key file paired with CertPath (PEM).
	KeyPath string `json:"key"`
	// CAPath is an optional CA bundle used to verify the peer (PEM).
	CAPath string `json:"ca"`
	// ServerName overrides the SNI / verification hostname
	// (default: the dialed host).
	ServerName string `json:"servername"`
	// VerifyServerCert disables peer verification when false
	// (Node-RED field name: verifyservercert).
	VerifyServerCert bool `json:"verifyservercert"`
}

// Build loads the configured material into a *tls.Config.
func (c *TLSConfig) Build() (*tls.Config, error) {
	out := &tls.Config{
		InsecureSkipVerify: !c.VerifyServerCert, //nolint:gosec // explicit user opt-out, Node-RED parity
		ServerName:         c.ServerName,
	}
	if c.CertPath != "" {
		pair, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("tlsConfig cert/key load failed: %w", err)
		}
		out.Certificates = []tls.Certificate{pair}
	}
	if c.CAPath != "" {
		pem, err := os.ReadFile(c.CAPath)
		if err != nil {
			return nil, fmt.Errorf("tlsConfig ca read failed: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tlsConfig ca %q contains no PEM certificates", c.CAPath)
		}
		out.RootCAs = pool
	}
	return out, nil
}
