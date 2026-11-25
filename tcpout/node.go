// Package tcpout contains the tcpOut node implementation.
package tcpout

import (
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(TCPOutConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewTCPOutNode(n, c, e), nil
}

// init registers the tcpOut node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "tcpOut",
			Name:        "TCP Out",
			Description: "Sends data over TCP, or replies on the incoming session",
			Icon:        "plug",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the tcpOut node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"beserver": map[string]interface{}{
			"type":    "string",
			"title":   "Type",
			"enum":    []string{"client", "server", "reply"},
			"default": "client",
			"description": "client: connect to host:port and keep the " +
				"connection; server: listen on port and write to every " +
				"connected client; reply: answer the tcpIn session that " +
				"produced the message",
		},
		"host": map[string]interface{}{
			"type":        "string",
			"title":       "Host",
			"description": "Target host (default 127.0.0.1)",
			"showIf":      map[string]interface{}{"beserver": "client"},
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "Target port (client) or listen port (server); unused for reply",
			"showIf":      map[string]interface{}{"beserver": []string{"client", "server"}},
		},
		"end": map[string]interface{}{
			"type":        "boolean",
			"title":       "Close Connection",
			"description": "Close the connection after each write",
			"default":     false,
			"showIf":      map[string]interface{}{"beserver": []string{"client", "server"}},
		},
		"base64": map[string]interface{}{
			"type":        "boolean",
			"title":       "Decode Base64",
			"description": "Decode a base64 payload string before sending",
			"default":     false,
		},
		"reply": map[string]interface{}{
			"type":        "boolean",
			"title":       "Reply to Session (legacy)",
			"description": "Legacy alias of Type=reply; ignored when Type is set",
		},
		"tls": map[string]interface{}{
			"type":        "string",
			"format":      "nodeselect",
			"peerType":    "tlsConfig",
			"title":       "TLS Config",
			"description": "Optional tlsConfig node; client-mode dials are upgraded to TLS with that material",
			"showIf":      map[string]interface{}{"beserver": "client"},
		},
	},
}

// TCPOutConfig holds configuration for the tcpOut node.
type TCPOutConfig struct {
	// Beserver is the Node-RED mode selector: "client" keeps one
	// connection to host:port, "server" listens on port and writes
	// to every connected socket, "reply" answers the tcpIn session
	// that produced the message (or broadcasts to all pooled
	// sessions when the message has no ip/port metadata).
	Beserver string `json:"beserver"`
	// Host is the target host for client mode (default 127.0.0.1).
	Host string `json:"host"`
	// Port is the target port (client) or listen port (server).
	Port int `json:"port"`
	// End closes the connection after each write (client and server
	// modes — Node-RED's "close connection" checkbox).
	End bool `json:"end"`
	// Base64 decodes a base64 payload string before sending.
	Base64 bool `json:"base64"`
	// Reply is the legacy flag form of Beserver "reply"; ignored
	// when Beserver is set.
	Reply bool `json:"reply"`
	// TLS names a tlsConfig node in the same workflow; client-mode
	// dials are upgraded to TLS with that material.
	TLS string `json:"tls"`
}

// mode resolves the write mode: beserver wins, legacy reply falls back.
func (c *TCPOutConfig) mode() string {
	if c.Beserver != "" {
		return c.Beserver
	}
	if c.Reply {
		return "reply"
	}
	return "client"
}

// tcpOutState is the per-node workflow state holding the long-lived
// resources — executors are rebuilt per message so the client
// connection, the server listener and its accepted sockets must live
// in host state.
type tcpOutState struct {
	mu       sync.Mutex
	conn     net.Conn            // client-mode persistent connection
	listener net.Listener        // server-mode listener
	conns    map[net.Conn]bool   // server-mode accepted sockets
}

// TCPOutNode writes the message Data over TCP — replying on the
// accepted connection(s) that produced the message (reply mode),
// holding a persistent connection to host:port (client mode) or
// broadcasting to every client of a socket it listens on (server
// mode).
type TCPOutNode struct {
	*nodebase.BaseNode
	config     *TCPOutConfig
	host       nodebase.NodeHost
	workflowID string
	stopOnce   sync.Once
	// state caches the per-node workflow state resolved in
	// Activate/Execute — Stop must not call GetOrCreateNodeState
	// because the runner holds the host lock while stopping.
	state *tcpOutState
}

// Execute sends the payload's Data and returns the input unchanged.
func (n *TCPOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	data, err := n.data(input)
	if err != nil {
		return nil, err
	}

	switch n.config.mode() {
	case "reply":
		return n.reply(input, data)
	case "server":
		return n.writeServer(data, input)
	default:
		return n.writeClient(data, input)
	}
}

// Activate resolves the per-node state up front (so Stop can reach it
// without touching host state) and, in server mode, binds the listen
// socket the same way tcpIn does. Client-mode connections dial lazily
// on the first message so a missing server does not fail the deploy.
func (n *TCPOutNode) Activate(ctx context.Context) error {
	n.state = n.nodeState()
	if n.config.mode() == "server" {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.config.Port))
		if err != nil {
			return fmt.Errorf("tcpOut cannot listen on :%d: %w", n.config.Port, err)
		}
		n.state.mu.Lock()
		n.state.listener = listener
		n.state.mu.Unlock()
		go n.acceptLoop(listener)
	}
	return nil
}

// StopKey dedupes stopper registration across the per-message
// executor shells — they all share the one state object.
func (n *TCPOutNode) StopKey() string { return "tcpOut:" + n.GetID() }

// Stop shuts down the server listener / accepted sockets / client
// connection (idempotent).
func (n *TCPOutNode) Stop() error {
	var err error
	n.stopOnce.Do(func() {
		if n.state == nil {
			return
		}
		n.state.mu.Lock()
		defer n.state.mu.Unlock()
		if n.state.listener != nil {
			err = n.state.listener.Close()
		}
		for conn := range n.state.conns {
			_ = conn.Close()
		}
		n.state.conns = nil
		if n.state.conn != nil {
			_ = n.state.conn.Close()
			n.state.conn = nil
		}
	})
	return err
}

// nodeState returns (creating if needed) the per-node workflow state.
func (n *TCPOutNode) nodeState() *tcpOutState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &tcpOutState{conns: map[net.Conn]bool{}}
	}).(*tcpOutState)
}

// acceptLoop accepts server-mode connections until the listener
// closes, tracking them for broadcast writes.
func (n *TCPOutNode) acceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return // listener closed (Stop) or fatal — either way we exit
		}
		conn.(*net.TCPConn).SetKeepAlive(true)
		n.state.mu.Lock()
		n.state.conns[conn] = true
		n.state.mu.Unlock()
		go func() {
			// Drop the socket from the pool when the client goes away —
			// reads are discarded (Node-RED ignores inbound data here).
			buf := make([]byte, 4096)
			for {
				if _, err := conn.Read(buf); err != nil {
					n.state.mu.Lock()
					delete(n.state.conns, conn)
					n.state.mu.Unlock()
					_ = conn.Close()
					return
				}
			}
		}()
	}
}

// data resolves the bytes to send: strings pass through unless
// base64 decode is configured, []byte as-is, anything else as JSON
// with a trailing newline (shared TCPPayloadBytes semantics).
func (n *TCPOutNode) data(input nodestore.MessageInterface) ([]byte, error) {
	if n.config.Base64 {
		if input != nil {
			if s, ok := input.GetPayload().(string); ok {
				raw, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					return nil, fmt.Errorf("tcpOut cannot decode base64 payload: %w", err)
				}
				return raw, nil
			}
		}
	}
	return nodebase.TCPPayloadBytes(input)
}

// writeClient sends on the persistent client-mode connection,
// dialing host:port on first use (and re-dialing after an End close
// or a dropped connection).
func (n *TCPOutNode) writeClient(data []byte, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.state = n.nodeState()
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	if n.state.conn == nil {
		conn, err := n.dial()
		if err != nil {
			return nil, err
		}
		n.state.conn = conn
	}
	if _, err := n.state.conn.Write(data); err != nil {
		_ = n.state.conn.Close()
		n.state.conn = nil
		return nil, fmt.Errorf("tcpOut write failed: %w", err)
	}
	if n.config.End {
		_ = n.state.conn.Close()
		n.state.conn = nil
	}
	return input, nil
}

// dial opens the client-mode connection to host:port, upgraded to
// TLS when the node references a tlsConfig node.
func (n *TCPOutNode) dial() (net.Conn, error) {
	host := n.config.Host
	if host == "" {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", n.config.Port))
	if n.config.TLS != "" {
		cfg, err := n.host.LoadTLSConfig(n.workflowID, n.config.TLS)
		if err != nil {
			return nil, err
		}
		if cfg.ServerName == "" {
			cfg = cfg.Clone()
			cfg.ServerName = host
		}
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, cfg)
		if err != nil {
			return nil, fmt.Errorf("tcpOut cannot connect to %s: %w", addr, err)
		}
		return conn, nil
	}
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("tcpOut cannot connect to %s: %w", addr, err)
	}
	return conn, nil
}

// writeServer broadcasts the data to every socket accepted by the
// server-mode listener; End closes each socket after the write.
func (n *TCPOutNode) writeServer(data []byte, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.state = n.nodeState()
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	if n.state.listener == nil {
		return nil, errors.New("tcpOut server mode is not listening")
	}
	for conn := range n.state.conns {
		if _, err := conn.Write(data); err != nil {
			_ = conn.Close()
			delete(n.state.conns, conn)
			continue
		}
		if n.config.End {
			_ = conn.Close()
			delete(n.state.conns, conn)
		}
	}
	return input, nil
}

// reply writes to the accepted tcpIn connection identified by the
// message's Metadata ip/port; with no session metadata it broadcasts
// to every pooled session (Node-RED connectionPool semantics).
func (n *TCPOutNode) reply(input nodestore.MessageInterface, data []byte) (nodestore.MessageInterface, error) {
	sessions := n.host.GetOrCreateNodeState(n.workflowID, nodebase.TCPSessionsKey, func() interface{} {
		return nodebase.NewTCPSessions()
	}).(*nodebase.TCPSessions)

	var ip, port string
	if input != nil && input.GetMetadata() != nil {
		ip, _ = input.GetMetadata()["ip"].(string)
		port, _ = input.GetMetadata()["port"].(string)
	}
	if ip == "" || port == "" {
		// No session selected — write to every pooled connection.
		sessions.Each(func(key string, conn net.Conn) {
			_, _ = conn.Write(data)
		})
		return input, nil
	}
	key := net.JoinHostPort(ip, port)
	conn, ok := sessions.Get(key)
	if !ok {
		return nil, fmt.Errorf("tcpOut: no open session for %s", key)
	}
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("tcpOut reply write to %s failed: %w", key, err)
	}
	return input, nil
}

// Validate checks the tcpOut node configuration
func (n *TCPOutNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	switch n.config.mode() {
	case "reply":
		return nil
	case "client", "server":
		if n.config.Port <= 0 || n.config.Port > 65535 {
			return fmt.Errorf("tcpOut requires a valid port (1-65535) unless reply mode is set")
		}
		return nil
	default:
		return fmt.Errorf("invalid tcpOut type %q (client|server|reply)", n.config.Beserver)
	}
}

// NewTCPOutNode creates a new tcpOut node
func NewTCPOutNode(node nodestore.NodeInterface, config *TCPOutConfig, host nodebase.NodeHost) *TCPOutNode {
	return &TCPOutNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}
