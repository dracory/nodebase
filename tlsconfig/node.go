// Package tlsconfig contains the tlsConfig node implementation.
package tlsconfig

import (
	"context"
	_ "embed"
	"errors"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(nodebase.TLSConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewTLSConfigNode(n, c), nil
}

// init registers the tlsConfig node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "tlsConfig",
			Name:        "TLS Config",
			Description: "Shared TLS material (cert/key/CA) referenced by tcp nodes",
			Icon:        "lock",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the tlsConfig node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"cert": map[string]string{
			"type":        "string",
			"title":       "Certificate",
			"description": "PEM certificate file; cert and key must be set together",
		},
		"key": map[string]string{
			"type":        "string",
			"title":       "Private Key",
			"description": "PEM private key file paired with the certificate",
		},
		"ca": map[string]string{
			"type":        "string",
			"title":       "CA Bundle",
			"description": "Optional PEM CA bundle used to verify the peer",
		},
		"servername": map[string]string{
			"type":        "string",
			"title":       "Server Name",
			"description": "SNI / verification hostname override (default: the dialed host)",
		},
		"verifyservercert": map[string]interface{}{
			"type":        "boolean",
			"title":       "Verify Peer Certificate",
			"description": "false disables peer verification (insecure; Node-RED verifyservercert)",
			"default":     true,
		},
	},
}

// TLSConfigNode is a config entity; its Execute is never invoked (no
// ports), existing only to satisfy the NodeExecutor contract.
type TLSConfigNode struct {
	*nodebase.BaseNode
	config *nodebase.TLSConfig
}

// NewTLSConfigNode creates a new tlsConfig node
func NewTLSConfigNode(node nodestore.NodeInterface, config *nodebase.TLSConfig) *TLSConfigNode {
	return &TLSConfigNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute passes the payload through — unreachable in practice since
// a tlsConfig node has no inputs.
func (n *TLSConfigNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// Validate checks the structural config: a cert requires its key and
// vice versa. File readability is checked when the material is loaded
// (LoadTLSConfig) — Node-RED similarly flags unreadable files at use.
func (n *TLSConfigNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if (n.config.CertPath == "") != (n.config.KeyPath == "") {
		return errors.New("tlsConfig: cert and key must be configured together")
	}
	return nil
}
