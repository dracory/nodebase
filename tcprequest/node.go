// Package tcprequest contains the tcpRequest node implementation.
package tcprequest

import (
	"bytes"
	"context"
	"crypto/tls"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(TCPRequestConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewTCPRequestNode(n, c, e), nil
}

// init registers the tcpRequest node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "tcpRequest",
			Name:        "TCP Request",
			Description: "Sends the payload over TCP and emits the server's reply",
			Icon:        "exchange-alt",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the tcpRequest node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"host": map[string]string{
			"type":        "string",
			"title":       "Host",
			"description": "Remote host to connect to (default 127.0.0.1)",
		},
		"port": map[string]interface{}{
			"type":  "integer",
			"title": "Port",
		},
		"ret": map[string]interface{}{
			"type":    "string",
			"title":   "Return Type",
			"enum":    []string{"string", "buffer"},
			"default": "string",
			"description": "string: reply payload as text; " +
				"buffer: raw bytes",
		},
		"mode": map[string]interface{}{
			"type":    "string",
			"title":   "Reply Framing",
			"enum":    []string{"timeout", "delimiter", "length"},
			"default": "timeout",
			"description": "timeout: take whatever arrives before Timeout; " +
				"delimiter: read until the delimiter bytes appear; " +
				"length: read exactly Length bytes",
		},
		"timeout": map[string]interface{}{
			"type":        "number",
			"title":       "Timeout (seconds)",
			"description": "Bounds connect + reply wait; 0 = 10 seconds",
			"default":     10,
		},
		"delimiter": map[string]interface{}{
			"type":        "string",
			"title":       "Delimiter",
			"description": "Reply terminator for delimiter mode (escape sequences like \\n allowed)",
			"showIf":      map[string]interface{}{"mode": "delimiter"},
		},
		"length": map[string]interface{}{
			"type":        "integer",
			"title":       "Length",
			"description": "Fixed reply size for length mode",
			"showIf":      map[string]interface{}{"mode": "length"},
		},
		"keep": map[string]interface{}{
			"type":        "boolean",
			"title":       "Keep Connection",
			"description": "Reuse the client connection across messages instead of connect-send-close per message",
			"default":     false,
		},
		"errorCatch": nodebase.ErrorCatchSchemaField(),
		"tls": map[string]interface{}{
			"type":        "string",
			"format":      "nodeselect",
			"peerType":    "tlsConfig",
			"title":       "TLS Config",
			"description": "Optional tlsConfig node; the connection is upgraded to TLS with that material",
		},
	},
	"required": []string{"port"},
}

// TCPRequestConfig holds configuration for the tcpRequest node — the
// Node-RED `tcp request` equivalent: a client session that connects to
// a server, sends the payload and waits for the reply.
type TCPRequestConfig struct {
	// Host is the remote host (default 127.0.0.1).
	Host string `json:"host"`
	// Port is the remote port.
	Port int `json:"port"`
	// Ret selects the reply payload type: "string" (default) | "buffer"
	// (raw []byte). Node-RED field name: ret.
	Ret string `json:"ret"`
	// Mode selects the reply framing: "timeout" (default — take
	// whatever arrives before Timeout), "delimiter" (read until the
	// delimiter bytes appear) or "length" (read exactly Length bytes).
	Mode string `json:"mode"`
	// Timeout bounds connect + reply wait, in seconds (default 10;
	// 0 = 10).
	Timeout float64 `json:"timeout"`
	// Delimiter terminates the reply in "delimiter" mode
	// (escape sequences like \n allowed).
	Delimiter string `json:"delimiter"`
	// Length is the fixed reply size in "length" mode.
	Length int `json:"length"`
	// Keep keeps the client connection open across messages instead of
	// closing it after each request/reply (Node-RED's persistent
	// client session). The connection is dropped on redeploy/stop.
	Keep bool `json:"keep"`
	// TLS names a tlsConfig node in the same workflow; when set the
	// connection is upgraded to TLS with that material.
	TLS string `json:"tls"`
}

// tcpRequestState is the per-node workflow state for keep-alive
// sessions — executors are rebuilt per message so the connection must
// live in host state.
type tcpRequestState struct {
	mu   sync.Mutex
	conn net.Conn
}

// TCPRequestNode sends the payload over a TCP connection and emits the
// server's reply as the message payload.
type TCPRequestNode struct {
	*nodebase.BaseNode
	config     *TCPRequestConfig
	host       nodebase.NodeHost
	workflowID string
}

// NewTCPRequestNode creates a new tcpRequest node
func NewTCPRequestNode(node nodestore.NodeInterface, config *TCPRequestConfig, host nodebase.NodeHost) *TCPRequestNode {
	return &TCPRequestNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute dials (or reuses a kept connection), writes the payload and
// reads the reply per the configured framing.
func (n *TCPRequestNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	data, err := nodebase.TCPPayloadBytes(input)
	if err != nil {
		return nil, err
	}
	timeout := n.config.Timeout
	if timeout <= 0 {
		timeout = 10
	}
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))

	var conn net.Conn
	var state *tcpRequestState
	if n.config.Keep && n.host != nil {
		state = n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
			return &tcpRequestState{}
		}).(*tcpRequestState)
		state.mu.Lock()
		defer state.mu.Unlock()
		conn = state.conn
	}
	if conn != nil {
		// Drop stale connections before reuse — a failed write would
		// otherwise surface one request late.
		if err := conn.SetReadDeadline(time.Now()); err == nil {
			one := make([]byte, 1)
			if _, rerr := conn.Read(one); rerr != nil && rerr != io.EOF {
				if nerr, ok := rerr.(net.Error); !ok || !nerr.Timeout() {
					_ = conn.Close()
					conn = nil
				}
			} else {
				// Unexpected buffered data — treat the connection as bad.
				_ = conn.Close()
				conn = nil
			}
		}
	}
	if conn == nil {
		conn, err = n.dial(deadline)
		if err != nil {
			return nil, err
		}
	}
	if state == nil {
		defer conn.Close()
	} else {
		state.conn = conn
	}

	if err := conn.SetWriteDeadline(deadline); err != nil {
		return nil, fmt.Errorf("tcpRequest write deadline failed: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		n.drop(state)
		return nil, fmt.Errorf("tcpRequest write to %s failed: %w", conn.RemoteAddr(), err)
	}

	reply, err := n.readReply(conn, deadline)
	if err != nil {
		n.drop(state)
		return nil, fmt.Errorf("tcpRequest read from %s failed: %w", conn.RemoteAddr(), err)
	}

	metadata := nodebase.CloneMetadata(input)
	if host, port, serr := net.SplitHostPort(conn.RemoteAddr().String()); serr == nil {
		metadata["ip"] = host
		metadata["port"] = port
	}

	var out interface{}
	if n.config.Ret == "buffer" {
		out = reply
	} else {
		out = string(reply)
	}
	return nodestore.NewMessage().SetPayload(out).SetMetadata(metadata).SetTimestamp(time.Now()), nil
}

// drop releases a broken kept connection.
func (n *TCPRequestNode) drop(state *tcpRequestState) {
	if state == nil {
		return
	}
	if state.conn != nil {
		_ = state.conn.Close()
		state.conn = nil
	}
}

// dial opens a TCP (or TLS) connection to host:port bounded by the
// request deadline.
func (n *TCPRequestNode) dial(deadline time.Time) (net.Conn, error) {
	host := n.config.Host
	if host == "" {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", n.config.Port))
	dialer := &net.Dialer{Deadline: deadline}
	if n.config.TLS != "" {
		cfg, err := n.host.LoadTLSConfig(n.workflowID, n.config.TLS)
		if err != nil {
			return nil, err
		}
		return tls.DialWithDialer(dialer, "tcp", addr, cfg)
	}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcpRequest cannot connect to %s: %w", addr, err)
	}
	return conn, nil
}

// readReply consumes the server reply per the configured framing.
func (n *TCPRequestNode) readReply(conn net.Conn, deadline time.Time) ([]byte, error) {
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	switch n.config.Mode {
	case "", "timeout":
		// Whatever arrives before the deadline is the reply; a
		// half-close (EOF) ends early.
		return io.ReadAll(conn)
	case "delimiter":
		delim := nodebase.UnescapeTCP(n.config.Delimiter)
		if delim == "" {
			return nil, errors.New("tcpRequest delimiter mode requires a delimiter")
		}
		return readUntil(conn, []byte(delim), deadline)
	case "length":
		if n.config.Length <= 0 {
			return nil, errors.New("tcpRequest length mode requires a positive length")
		}
		buf := make([]byte, n.config.Length)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return nil, err
		}
		return buf, nil
	default:
		return nil, fmt.Errorf("invalid tcpRequest mode %q (timeout|delimiter|length)", n.config.Mode)
	}
}

// readUntil reads until delim appears (and includes it) or the deadline
// trips. io.ReadAll on a deadline cannot do this directly.
func readUntil(conn net.Conn, delim []byte, deadline time.Time) ([]byte, error) {
	var buf []byte
	tmp := make([]byte, 4096)
	for {
		if idx := bytes.Index(buf, delim); idx >= 0 {
			// The reply ends at the first delimiter (inclusive);
			// anything past it belongs to a later response.
			return buf[:idx+len(delim)], nil
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
		n, err := conn.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			return nil, err
		}
	}
}

// Validate checks the tcpRequest node configuration
func (n *TCPRequestNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Port <= 0 || n.config.Port > 65535 {
		return fmt.Errorf("tcpRequest requires a valid port (1-65535)")
	}
	switch n.config.Mode {
	case "", "timeout", "delimiter", "length":
	default:
		return fmt.Errorf("invalid tcpRequest mode %q (timeout|delimiter|length)", n.config.Mode)
	}
	if n.config.Mode == "delimiter" && nodebase.UnescapeTCP(n.config.Delimiter) == "" {
		return errors.New("tcpRequest delimiter mode requires a delimiter")
	}
	if n.config.Mode == "length" && n.config.Length <= 0 {
		return errors.New("tcpRequest length mode requires a positive length")
	}
	switch n.config.Ret {
	case "", "string", "buffer":
	default:
		return fmt.Errorf("invalid tcpRequest ret %q (string|buffer)", n.config.Ret)
	}
	return nil
}
