// Package notify contains the distributed notify node: it atomically
// increments a durable release signal shared with wait nodes.
package notify

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/dracory/nodebase"
	nodetemplate "github.com/dracory/nodebase/template"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, host nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &Config{SignalTTLSeconds: 0}
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	if err := validateConfig(c); err != nil {
		return nil, err
	}
	return NewNode(n, c, host), nil
}

func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "notify",
			Name:        "Notify",
			Description: "Atomically increments a durable distributed release signal",
			Icon:        "bell",
			Category:    "Flow Control",
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
		"signal": map[string]interface{}{
			"type": "string", "title": "Release Signal",
			"description": "Fixed text or a mustache template such as {{payload.id}}; the evaluated non-empty value is the distributed signal key",
		},
		"cacheKeys": map[string]interface{}{
			"type": "string", "title": "Metadata Keys To Cache",
			"description": "Comma-separated message metadata keys copied into the signal cache for released wait messages; nested metadata paths use dots",
		},
		"signalTTLSeconds": map[string]interface{}{
			"type": "number", "title": "Signal TTL Seconds", "default": 0,
			"description": "0 keeps unconsumed signal counts indefinitely; positive values expire the whole unconsumed count after the latest increment",
		},
	},
}

// Config holds the notify node configuration.
type Config struct {
	Signal           string      `json:"signal"`
	CacheKeys        string      `json:"cacheKeys"`
	SignalTTLSeconds interface{} `json:"signalTTLSeconds"`
}

// Node increments distributed release signals.
type Node struct {
	*nodebase.BaseNode
	cfg  *Config
	host nodebase.NodeHost
}

// NewNode creates a notify node.
func NewNode(node nodestore.NodeInterface, cfg *Config, host nodebase.NodeHost) *Node {
	return &Node{BaseNode: nodebase.NewBaseNode(node), cfg: cfg, host: host}
}

func validateConfig(c *Config) error {
	if c == nil {
		return fmt.Errorf("notify: missing configuration")
	}
	if strings.TrimSpace(c.Signal) == "" {
		return fmt.Errorf("notify: release signal template is required")
	}
	if _, err := signalTTL(c); err != nil {
		return err
	}
	return nil
}

func signalTTL(c *Config) (time.Duration, error) {
	if c.SignalTTLSeconds == nil || c.SignalTTLSeconds == "" {
		return 0, nil
	}
	seconds, ok := nodebase.ParseNumeric(c.SignalTTLSeconds)
	if !ok || seconds < 0 {
		return 0, fmt.Errorf("notify: signal TTL must be a non-negative number of seconds")
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

// Validate fails deployment early when the host offers no durable
// coordination store. There is deliberately no in-memory fallback.
func (n *Node) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if err := validateConfig(n.cfg); err != nil {
		return err
	}
	_, err := nodebase.ResolveCoordinationStore(n.host)
	return err
}

// Execute evaluates the signal template, copies the configured metadata
// keys into the signal cache, and atomically increments the durable signal.
func (n *Node) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if input == nil {
		return nil, fmt.Errorf("notify: input message is required")
	}
	if err := validateConfig(n.cfg); err != nil {
		return nil, err
	}
	store, err := nodebase.ResolveCoordinationStore(n.host)
	if err != nil {
		return nil, err
	}
	key, err := nodetemplate.RenderMustache(n.cfg.Signal, input, n.host, n.GetWorkflowID(), n.GetID(), false)
	if err != nil {
		return nil, fmt.Errorf("notify: signal template: %w", err)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("notify: evaluated release signal is blank")
	}
	ttl, err := signalTTL(n.cfg)
	if err != nil {
		return nil, err
	}
	metadata := selectedMetadata(input, n.cfg.CacheKeys)
	if err := store.IncrementSignal(ctx, key, metadata, ttl); err != nil {
		return nil, fmt.Errorf("notify: increment signal %q: %w", key, err)
	}
	return input, nil
}

// selectedMetadata copies the comma-separated metadata keys. A leading
// "msg." is accepted for consistency with other node properties but is not
// stored as part of the cache key. Missing keys are ignored.
func selectedMetadata(input nodestore.MessageInterface, keys string) map[string]interface{} {
	out := map[string]interface{}{}
	for _, key := range strings.Split(keys, ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		key = strings.TrimPrefix(key, "msg.")
		if key == "" {
			continue
		}
		if value, ok := nodebase.GetPath(input.GetMetadata(), key); ok {
			out[key] = value
		}
	}
	return out
}
