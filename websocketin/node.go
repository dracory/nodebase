// Package websocketin contains the websocketIn node implementation.
package websocketin

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/coder/websocket"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(WebSocketInConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewWebSocketInNode(n, c, e), nil
}

// init registers the websocketIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "websocketIn",
			Name:        "WS In",
			Description: "Accepts websocket connections on its own listener and emits received messages",
			Icon:        "plug",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the websocketIn node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"path": map[string]interface{}{
			"type":        "string",
			"title":       "Path",
			"description": "Endpoint path that accepts websocket upgrades",
			"default":     "/ws",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "The node's own listen port",
			"default":     8082,
		},
		"allowedOrigins": map[string]interface{}{
			"type":        "string",
			"title":       "Allowed Origins",
			"description": "Comma-separated Origin host patterns allowed to connect (e.g. \"example.com,*.example.com\"). Empty accepts any origin — cross-site websocket hijacking exposure; set this on browser-facing flows",
			"default":     "",
		},
	},
}

// WebSocketInConfig holds configuration for the websocketIn node.
type WebSocketInConfig struct {
	// Path is the endpoint path that accepts upgrades (default "/ws").
	Path string `json:"path"`
	// Port is the node's own listen port (default 8082). Like
	// httpTrigger the node binds its own listener rather than sharing
	// the admin mux, which flowrunner has no reference to.
	Port int `json:"port"`
	// AllowedOrigins is a comma-separated list of Origin host
	// patterns passed to websocket.Accept. Empty keeps the legacy
	// any-origin behaviour (Node-RED parity, but a CSWSH exposure —
	// set it whenever browsers on foreign origins must not connect).
	AllowedOrigins string `json:"allowedOrigins"`
	// Server references a shared websocketListener config node id
	// (Node-RED "server"): the node receives frames from clients of
	// that shared endpoint instead of binding its own port.
	Server string `json:"server"`
	// Client references a shared websocketClient config node id
	// (Node-RED "client"): the node dials out and emits whatever the
	// remote server sends (client mode). Client wins over Server.
	Client string `json:"client"`
}

// WebSocketInNode runs its own HTTP server that upgrades connections on
// the configured path and emits every received ws message downstream
// with the connection's session id in Metadata.
type WebSocketInNode struct {
	*nodebase.BaseNode
	config     *WebSocketInConfig
	host       nodebase.NodeHost
	workflowID string
	server     *http.Server
	stopOnce   sync.Once
	seq        int64
	// sess caches the session registry resolved in Execute — Stop must
	// not call host state (StopWorkflow holds the host lock).
	sess *nodebase.WSSessions
	// listener/client hold the shared-config state in Node-RED
	// server/client mode; done stops the client dial-retry loop.
	listener *nodebase.WSListenerState
	client   *nodebase.WSClientState
	done     chan struct{}
}

// Execute binds the listener and starts the HTTP server. Binding is
// synchronous so a port conflict surfaces as a deploy error.
func (n *WebSocketInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.sess = n.sessions()

	// Node-RED shared-config modes take precedence over the private
	// listener: a referenced config node means the endpoint (server
	// mode) or the remote peer (client mode) lives elsewhere.
	if n.config.Client != "" {
		return n.executeClientMode()
	}
	if n.config.Server != "" {
		return n.executeServerMode()
	}

	path := n.config.Path
	if path == "" {
		path = "/ws"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	port := n.config.Port
	if port <= 0 {
		port = 8082 // must not collide with admin :8080 / trigger :8081
	}

	handler := http.NewServeMux()
	handler.HandleFunc(path, n.handleUpgrade)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("websocketIn cannot listen on :%d: %w", port, err)
	}
	n.server = &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: handler}
	go func() {
		if err := n.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("websocketIn server error: %v\n", err)
		}
	}()

	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening", "port": port, "path": path}).SetTimestamp(time.Now()), nil
}

// acceptOptions builds the websocket.Accept options from config: an
// origin allowlist when configured, else the legacy any-origin mode.
func (n *WebSocketInNode) acceptOptions() *websocket.AcceptOptions {
	var patterns []string
	for _, p := range strings.Split(n.config.AllowedOrigins, ",") {
		if p = strings.TrimSpace(p); p != "" {
			patterns = append(patterns, p)
		}
	}
	if len(patterns) > 0 {
		return &websocket.AcceptOptions{OriginPatterns: patterns}
	}
	// Flows may serve browsers on any origin; the editor is not
	// necessarily same-origin with this node's own listener.
	return &websocket.AcceptOptions{InsecureSkipVerify: true} //nolint:gosec — legacy default; set allowedOrigins to restrict
}

// handleUpgrade accepts the websocket upgrade, registers the session
// and runs the read loop until the peer disconnects.
func (n *WebSocketInNode) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, n.acceptOptions())
	if err != nil {
		return // Accept already wrote an error response
	}

	sessionID := fmt.Sprintf("ws-%d", atomic.AddInt64(&n.seq, 1))
	n.sess.Put(sessionID, conn)
	defer func() {
		n.sess.DeleteIf(sessionID, conn)
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}()

	for {
		typ, data, err := conn.Read(r.Context())
		if err != nil {
			return // peer closed or network failure — drop the session
		}
		payload := nodestore.NewMessage().SetMetadata(map[string]interface{}{
			"sessionId": sessionID,
			"binary":    typ == websocket.MessageBinary,
		}).SetTimestamp(time.Now())
		if typ == websocket.MessageText {
			payload.SetPayload(string(data))
		} else {
			payload.SetPayload(data)
		}
		_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
	}
}

// executeServerMode attaches to the shared websocketListener endpoint:
// the upgrade route is mounted on the host's shared HTTP router by
// whichever dependent reaches it first, and every frame received on a
// mounted connection is emitted downstream.
func (n *WebSocketInNode) executeServerMode() (nodestore.MessageInterface, error) {
	state, cfg, err := nodebase.ResolveWSListener(n.host, n.workflowID, n.config.Server)
	if err != nil {
		return nil, err
	}
	if err := state.Attach(n.host); err != nil {
		return nil, err
	}
	n.listener = state
	state.Subscribe(n.GetID(), func(sessionID string, typ websocket.MessageType, data []byte) {
		n.emitFrame(sessionID, typ, data)
	})
	return nodestore.NewMessage().SetPayload(map[string]interface{}{
		"status": "listening", "path": cfg.Path, "server": n.config.Server,
	}).SetTimestamp(time.Now()), nil
}

// executeClientMode attaches to the shared websocketClient connection
// and starts the dial-retry loop in the background. Activation cannot
// block on the dial — a listener mounted later in the same activation
// pass (or a remote server that comes up after deploy) would deadlock
// the sequential pass otherwise.
func (n *WebSocketInNode) executeClientMode() (nodestore.MessageInterface, error) {
	state, cfg, err := nodebase.ResolveWSClient(n.host, n.workflowID, n.config.Client)
	if err != nil {
		return nil, err
	}
	state.Attach()
	n.client = state
	n.done = make(chan struct{})
	state.Subscribe(n.GetID(), func(_ string, typ websocket.MessageType, data []byte) {
		n.emitFrame("", typ, data)
	})
	go n.dialLoop(state)
	return nodestore.NewMessage().SetPayload(map[string]interface{}{
		"status": "connecting", "url": cfg.Path, "client": n.config.Client,
	}).SetTimestamp(time.Now()), nil
}

// dialLoop keeps the shared client connection up until Stop: dial,
// wait for the read pump to drop the conn, re-dial with backoff.
func (n *WebSocketInNode) dialLoop(state *nodebase.WSClientState) {
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

// emitFrame wraps a received ws frame as a message and emits it
// downstream with the connection's session id in Metadata.
func (n *WebSocketInNode) emitFrame(sessionID string, typ websocket.MessageType, data []byte) {
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{
		"sessionId": sessionID,
		"binary":    typ == websocket.MessageBinary,
	}).SetTimestamp(time.Now())
	if typ == websocket.MessageText {
		msg.SetPayload(string(data))
	} else {
		msg.SetPayload(data)
	}
	_ = n.host.Emit(n.workflowID, n.GetID(), msg, nil)
}

// sessions returns (creating if needed) the workflow-level session
// registry shared with websocketOut.
func (n *WebSocketInNode) sessions() *nodebase.WSSessions {
	return n.host.GetOrCreateNodeState(n.workflowID, nodebase.WSSessionsKey, func() interface{} {
		return nodebase.NewWSSessions()
	}).(*nodebase.WSSessions)
}

// Stop shuts down the HTTP server and closes every registered session.
func (n *WebSocketInNode) Stop() error {
	var err error
	n.stopOnce.Do(func() {
		if n.done != nil {
			close(n.done)
		}
		if n.client != nil {
			n.client.Unsubscribe(n.GetID())
			n.client.Release()
		}
		if n.listener != nil {
			n.listener.Unsubscribe(n.GetID())
			n.listener.Release()
		}
		if n.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = n.server.Shutdown(ctx)
		}
		if n.sess != nil {
			n.sess.CloseAllWith(websocket.StatusGoingAway, "workflow stopped")
		}
	})
	return err
}

// Validate checks the websocketIn node configuration
func (n *WebSocketInNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Port < 0 || n.config.Port > 65535 {
		return fmt.Errorf("websocketIn requires a valid port (0-65535; 0 defaults to 8082)")
	}
	return nil
}

// NewWebSocketInNode creates a new websocketIn node
func NewWebSocketInNode(node nodestore.NodeInterface, config *WebSocketInConfig, host nodebase.NodeHost) *WebSocketInNode {
	return &WebSocketInNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute binds the listener and starts the HTTP server. Binding is
// synchronous so a port conflict surfaces as a deploy error.
