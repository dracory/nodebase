// Package websocketout contains the websocketOut node implementation.
package websocketout

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(WebSocketOutConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewWebSocketOutNode(n, c, e), nil
}

// init registers the websocketOut node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "websocketOut",
			Name:        "WS Out",
			Description: "Writes the message to a websocket session or broadcasts to all",
			Icon:        "plug",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the websocketOut node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"target": map[string]interface{}{
			"type":    "string",
			"title":   "Target",
			"enum":    []string{"session", "broadcast"},
			"default": "session",
			"description": "session: write back on the connection named by the message's " +
				"sessionId metadata; broadcast: write to every open session",
		},
	},
}

// WebSocketOutConfig holds configuration for the websocketOut node.
type WebSocketOutConfig struct {
	// Target selects "session" (write back on the connection named by
	// the message's sessionId metadata) or "broadcast" (write to every
	// open session of this workflow's websocketIn nodes).
	Target string `json:"target"`
}

// WebSocketOutNode writes the message Data to websocket sessions held
// in the workflow's shared WSSessions registry.
type WebSocketOutNode struct {
	*nodebase.BaseNode
	config     *WebSocketOutConfig
	host       nodebase.NodeHost
	workflowID string
}

// Execute writes Data as a text message (binary for []byte) to the
// selected session(s) and returns the input unchanged.
func (n *WebSocketOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	typ, data, err := wsPayload(input)
	if err != nil {
		return nil, err
	}

	sessions := n.host.GetOrCreateNodeState(n.workflowID, nodebase.WSSessionsKey, func() interface{} {
		return nodebase.NewWSSessions()
	}).(*nodebase.WSSessions)

	target := n.config.Target
	if target == "" {
		target = "session"
	}

	switch target {
	case "broadcast":
		conns := sessions.All()
		if len(conns) == 0 {
			return nil, errors.New("websocketOut broadcast: no open sessions")
		}
		for _, conn := range conns {
			_ = conn.Write(ctx, typ, data) // best-effort fan-out
		}
		return input, nil
	case "session":
		if input == nil || input.GetMetadata() == nil {
			return nil, errors.New("websocketOut session mode requires message sessionId metadata")
		}
		sessionID, _ := input.GetMetadata()["sessionId"].(string)
		if sessionID == "" {
			return nil, errors.New("websocketOut session mode requires message sessionId metadata")
		}
		conn, ok := sessions.Get(sessionID)
		if !ok {
			return nil, fmt.Errorf("websocketOut: no open session %q", sessionID)
		}
		if err := conn.Write(ctx, typ, data); err != nil {
			return nil, fmt.Errorf("websocketOut write to %q failed: %w", sessionID, err)
		}
		return input, nil
	default:
		return nil, fmt.Errorf("invalid websocketOut target %q (session|broadcast)", target)
	}
}

// Validate checks the websocketOut node configuration
func (n *WebSocketOutNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	switch n.config.Target {
	case "", "session", "broadcast":
		return nil
	default:
		return fmt.Errorf("invalid websocketOut target %q (session|broadcast)", n.config.Target)
	}
}

// wsPayload converts message Data into a websocket frame: strings go
// out as text, []byte as binary, everything else as JSON text.
func wsPayload(input nodestore.MessageInterface) (websocket.MessageType, []byte, error) {
	if input == nil || input.GetPayload() == nil {
		return 0, nil, errors.New("websocketOut requires message data")
	}
	switch v := input.GetPayload().(type) {
	case string:
		return websocket.MessageText, []byte(v), nil
	case []byte:
		return websocket.MessageBinary, v, nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return 0, nil, fmt.Errorf("websocketOut cannot serialize data: %w", err)
		}
		return websocket.MessageText, raw, nil
	}
}

// NewWebSocketOutNode creates a new websocketOut node
func NewWebSocketOutNode(node nodestore.NodeInterface, config *WebSocketOutConfig, host nodebase.NodeHost) *WebSocketOutNode {
	return &WebSocketOutNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute writes Data as a text message (binary for []byte) to the
// selected session(s) and returns the input unchanged.
