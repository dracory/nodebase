// Package delay contains the delay node implementation.
package delay

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(DelayConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewDelayNode(n, c), nil
}

// init registers the delay node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "delay",
			Name:        "Delay",
			Description: "Pauses the workflow for a specified duration",
			Icon:        "clock",
			Category:    "Flow Control",
			Set:         "core",
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// DelayConfig holds configuration for the delay node. Duration is in
// milliseconds; when unset a small default is used.
type DelayConfig struct {
	Duration int `json:"duration"`
}

// DelayNode pauses execution for the configured duration, honoring
// context cancellation so a stopped workflow exits promptly
type DelayNode struct {
	*nodebase.BaseNode
	config *DelayConfig
}

// NewDelayNode creates a new delay node
func NewDelayNode(node nodestore.NodeInterface, config *DelayConfig) *DelayNode {
	return &DelayNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute sleeps for the configured duration then passes the payload on
func (n *DelayNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	duration := n.config.Duration
	if duration <= 0 {
		duration = 1000 // default 1s
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("delay interrupted: %w", ctx.Err())
	case <-time.After(time.Duration(duration) * time.Millisecond):
		return input, nil
	}
}
