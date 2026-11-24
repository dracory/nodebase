// Package websocketin contains the websocketIn node implementation.
package websocketin

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/coder/websocket"
	"net"
	"net/http"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"strings"
	"sync"
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
}

// Execute binds the listener and starts the HTTP server. Binding is
// synchronous so a port conflict surfaces as a deploy error.
func (n *WebSocketInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.sess = n.sessions()

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

// handleUpgrade accepts the websocket upgrade, registers the session
// and runs the read loop until the peer disconnects.
func (n *WebSocketInNode) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Flows may serve browsers on any origin; the editor is not
		// necessarily same-origin with this node's own listener.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return // Accept already wrote an error response
	}

	n.seq++
	sessionID := fmt.Sprintf("ws-%d", n.seq)
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
