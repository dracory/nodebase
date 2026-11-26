// Package websocketout contains the websocketOut node implementation.
package websocketout

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"

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
	// open session of this workflow's websocketIn nodes). Only used in
	// legacy mode — a server/client reference overrides it.
	Target string `json:"target"`
	// Server references a shared websocketListener config node id
	// (Node-RED "server"): writes go to the session named by the
	// message's sessionId metadata, or to every client of the shared
	// endpoint when no session is named.
	Server string `json:"server"`
	// Client references a shared websocketClient config node id
	// (Node-RED "client"): writes go over the shared outbound
	// connection. Client wins over Server.
	Client string `json:"client"`
}

// WebSocketOutNode writes the message Data to websocket sessions held
// in the workflow's shared WSSessions registry.
type WebSocketOutNode struct {
	*nodebase.BaseNode
	config     *WebSocketOutConfig
	host       nodebase.NodeHost
	workflowID string
	// listener/client hold the shared-config state in Node-RED
	// server/client mode; done stops the client dial-retry loop.
	listener *nodebase.WSListenerState
	client   *nodebase.WSClientState
	done     chan struct{}
	stopOnce sync.Once
}

// Activate warms the shared config resources at deploy time: a
// server reference mounts the listener endpoint now (clients must be
// able to connect before the first message arrives); a client
// reference starts the dial-retry loop.
func (n *WebSocketOutNode) Activate(ctx context.Context) error {
	if n.config.Client != "" {
		state, _, err := nodebase.ResolveWSClient(n.host, n.workflowID, n.config.Client)
		if err != nil {
			return err
		}
		state.Attach()
		n.client = state
		n.done = make(chan struct{})
		go n.dialLoop(state)
		return nil
	}
	if n.config.Server != "" {
		state, _, err := nodebase.ResolveWSListener(n.host, n.workflowID, n.config.Server)
		if err != nil {
			return err
		}
		if err := state.Attach(n.host); err != nil {
			return err
		}
		n.listener = state
	}
	return nil
}

// dialLoop keeps the shared client connection up until Stop, matching
// websocketIn's client-mode loop (either side may be the only
// dependent of the config node).
func (n *WebSocketOutNode) dialLoop(state *nodebase.WSClientState) {
	for {
		select {
		case <-n.done:
			return
		default:
		}
		if state.Conn() == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = state.Connect(ctx)
			cancel()
		}
		select {
		case <-n.done:
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Stop releases the shared config resources (idempotent).
func (n *WebSocketOutNode) Stop() error {
	n.stopOnce.Do(func() {
		if n.done != nil {
			close(n.done)
		}
		if n.client != nil {
			n.client.Release()
		}
		if n.listener != nil {
			n.listener.Release()
		}
	})
	return nil
}

// Execute writes Data as a text message (binary for []byte) to the
// selected session(s) and returns the input unchanged.
func (n *WebSocketOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	typ, data, err := wsPayload(input)
	if err != nil {
		return nil, err
	}

	// Executors may be rebuilt between activation and message delivery;
	// resolve the shared state lazily (Activate holds the attach ref).
	if n.client == nil && n.config.Client != "" {
		if state, _, err := nodebase.ResolveWSClient(n.host, n.workflowID, n.config.Client); err == nil {
			n.client = state
		}
	}
	if n.listener == nil && n.config.Server != "" {
		if state, _, err := nodebase.ResolveWSListener(n.host, n.workflowID, n.config.Server); err == nil {
			n.listener = state
		}
	}

	// Shared client mode: write over the dialed connection.
	if n.client != nil {
		conn := n.client.Conn()
		if conn == nil {
			return nil, errors.New("websocketOut client mode: not connected")
		}
		if err := conn.Write(ctx, typ, data); err != nil {
			return nil, fmt.Errorf("websocketOut client write failed: %w", err)
		}
		return input, nil
	}

	// Shared server mode: sessionId metadata selects the client, else
	// the message broadcasts to every client of the listener. A
	// deploy-time mount means the endpoint exists but a client may
	// connect a beat after the first message — wait briefly so the
	// looped-back client (websocketIn client mode) is not raced.
	if n.listener != nil {
		if sessionID, _ := messageSessionID(input); sessionID != "" {
			if conn, ok := n.listener.Conn(sessionID); ok {
				if err := conn.Write(ctx, typ, data); err != nil {
					return nil, fmt.Errorf("websocketOut write to %q failed: %w", sessionID, err)
				}
			}
			return input, nil // named session absent or write done — no broadcast
		}
		conns := n.waitForListenerConns(1500 * time.Millisecond)
		for _, id := range conns {
			if conn, ok := n.listener.Conn(id); ok {
				_ = conn.Write(ctx, typ, data) // best-effort fan-out
			}
		}
		return input, nil
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
		raw, err := nodebase.PayloadBytes(v)
		if err != nil {
			return 0, nil, fmt.Errorf("websocketOut cannot serialize data: %w", err)
		}
		return websocket.MessageText, raw, nil
	}
}

// messageSessionID extracts the sessionId metadata websocketIn stamps
// on received messages (empty when absent).
func messageSessionID(input nodestore.MessageInterface) (string, bool) {
	if input == nil || input.GetMetadata() == nil {
		return "", false
	}
	id, _ := input.GetMetadata()["sessionId"].(string)
	return id, id != ""
}

// waitForListenerConns polls the shared listener until a client is
// connected or the budget expires — the first message after deploy can
// race a looped-back client still completing its handshake.
func (n *WebSocketOutNode) waitForListenerConns(budget time.Duration) []string {
	deadline := time.Now().Add(budget)
	for {
		if conns := n.listener.Conns(); len(conns) > 0 {
			return conns
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-n.done:
			return nil
		case <-time.After(25 * time.Millisecond):
		}
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
