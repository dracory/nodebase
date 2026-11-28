// Package uuid contains the UUID node: RFC 9562 UUID generation using
// Go's standard-library uuid package.
package uuid

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	stdlibuuid "uuid"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &Config{Version: "v7", Target: "payload"}
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
			Type:        "uuid",
			Name:        "UUID",
			Description: "Generates a fresh RFC 9562 UUID for each message",
			Icon:        "key",
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
		"version": map[string]interface{}{
			"type": "string", "title": "UUID Version", "enum": []string{"v7", "v4"}, "default": "v7",
			"description": "v7 is time-ordered and preferred for new identifiers; v4 is random for compatibility with systems that expect random UUIDs",
		},
		"target": map[string]interface{}{
			"type": "string", "title": "Target Property", "default": "payload",
			"description": "Where the canonical lowercase UUID string is written; use payload to replace the payload or msg.* for metadata output",
		},
	},
}

// Config holds the UUID node configuration.
type Config struct {
	Version string `json:"version"`
	Target  string `json:"target"`
}

// Node writes a freshly generated UUID into each input message.
type Node struct {
	*nodebase.BaseNode
	cfg *Config
}

// NewNode creates a UUID node.
func NewNode(node nodestore.NodeInterface, cfg *Config) *Node {
	return &Node{BaseNode: nodebase.NewBaseNode(node), cfg: cfg}
}

func validateConfig(c *Config) error {
	if c == nil {
		return fmt.Errorf("uuid: missing configuration")
	}
	if c.Version != "v7" && c.Version != "v4" {
		return fmt.Errorf("uuid: unsupported version %q", c.Version)
	}
	if strings.TrimSpace(c.Target) == "" {
		return fmt.Errorf("uuid: target property is required")
	}
	return nil
}

// Execute generates one fresh UUID for this input message. Generation is
// deliberately independent of the input payload so every retry, branch, and
// replay gets a new identifier unless the workflow itself persists one.
func (n *Node) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if err := validateConfig(n.cfg); err != nil {
		return nil, err
	}
	var id stdlibuuid.UUID
	if n.cfg.Version == "v4" {
		id = stdlibuuid.NewV4()
	} else {
		id = stdlibuuid.NewV7()
	}
	if err := nodebase.SetMessageProperty(in, n.cfg.Target, id.String()); err != nil {
		return nil, err
	}
	return in, nil
}
