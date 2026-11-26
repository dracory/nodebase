// Package hash contains the hash node: deterministic whole-property
// hashing with standard-library cryptographic hash algorithms.
package hash

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	hasher "hash"
	"strings"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &Config{Algorithm: "sha256", Source: "payload", Target: "msg.hash", Encoding: "hex"}
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	if err := validateConfig(c); err != nil {
		return nil, err
	}
	return NewNode(n, c), nil
}

func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "hash",
			Name:        "Hash",
			Description: "Hashes a whole message property with sha256, sha1, sha512, or legacy md5",
			Icon:        "fingerprint",
			Category:    "Transform",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"algorithm": map[string]interface{}{
			"type": "string", "title": "Algorithm", "enum": []string{"sha256", "sha1", "sha512", "md5"}, "default": "sha256",
			"description": "md5 is legacy and is not collision-resistant; prefer sha256 or sha512 for security-sensitive use",
		},
		"source": map[string]interface{}{
			"type": "string", "title": "Source Property", "default": "payload",
			"description": "Property to hash as one whole byte sequence",
		},
		"target": map[string]interface{}{
			"type": "string", "title": "Target Property", "default": "msg.hash",
			"description": "Where the encoded hash is written; use payload to replace the payload or msg.* for metadata/attribute-style output",
		},
		"encoding": map[string]interface{}{
			"type": "string", "title": "Output Encoding", "enum": []string{"hex", "base64"}, "default": "hex",
		},
	},
}

// Config holds the hash node configuration.
type Config struct {
	Algorithm string `json:"algorithm"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	Encoding  string `json:"encoding"`
}

// Node hashes one configured message property into another.
type Node struct {
	*nodebase.BaseNode
	cfg *Config
}

// NewNode creates a hash node.
func NewNode(node nodestore.NodeInterface, cfg *Config) *Node {
	return &Node{BaseNode: nodebase.NewBaseNode(node), cfg: cfg}
}

func validateConfig(c *Config) error {
	if c == nil {
		return fmt.Errorf("hash: missing configuration")
	}
	if _, err := hashConstructor(c.Algorithm); err != nil {
		return err
	}
	if c.Encoding != "hex" && c.Encoding != "base64" {
		return fmt.Errorf("hash: unsupported output encoding %q", c.Encoding)
	}
	if strings.TrimSpace(c.Source) == "" {
		return fmt.Errorf("hash: source property is required")
	}
	if strings.TrimSpace(c.Target) == "" {
		return fmt.Errorf("hash: target property is required")
	}
	return nil
}

func hashConstructor(algorithm string) (func() hasher.Hash, error) {
	switch algorithm {
	case "sha256":
		return sha256.New, nil
	case "sha1":
		return sha1.New, nil
	case "sha512":
		return sha512.New, nil
	case "md5":
		return md5.New, nil
	default:
		return nil, fmt.Errorf("hash: unsupported algorithm %q", algorithm)
	}
}

// Execute hashes the configured source property. Input byte semantics are
// deterministic: strings are UTF-8 bytes, []byte passes through unchanged,
// and objects/arrays use the compact deterministic JSON encoding provided by
// encoding/json (including sorted object keys). Non-JSON values are errors.
func (n *Node) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if err := validateConfig(n.cfg); err != nil {
		return nil, err
	}
	value, ok := nodebase.LookupMessageProperty(in, n.cfg.Source)
	if !ok {
		return nil, fmt.Errorf("hash: source property %q not found", n.cfg.Source)
	}
	raw, err := nodebase.PayloadBytes(value)
	if err != nil {
		return nil, fmt.Errorf("hash: unsupported source type %T: %w", value, err)
	}
	constructor, _ := hashConstructor(n.cfg.Algorithm)
	h := constructor()
	_, _ = h.Write(raw)
	sum := h.Sum(nil)
	var encoded string
	if n.cfg.Encoding == "base64" {
		encoded = base64.StdEncoding.EncodeToString(sum)
	} else {
		encoded = hex.EncodeToString(sum)
	}
	if err := nodebase.SetMessageProperty(in, n.cfg.Target, encoded); err != nil {
		return nil, err
	}
	return in, nil
}
