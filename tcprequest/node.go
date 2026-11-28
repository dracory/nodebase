// Package tcprequest contains the tcpRequest node implementation.
package tcprequest

import (
	"bytes"
	"context"
	"crypto/tls"
	_ "embed"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"io"
	"net"
	"strings"
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
		"server": map[string]interface{}{
			"type":        "string",
			"title":       "Server",
			"description": "Remote host to connect to (Node-RED field name; host is the legacy alias)",
		},
		"host": map[string]string{
			"type":        "string",
			"title":       "Host",
			"description": "Legacy alias of Server; ignored when Server is set",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "Remote port; the message's port metadata overrides it",
		},
		"ret": map[string]interface{}{
			"type":    "string",
			"title":   "Return Type",
			"enum":    []string{"string", "buffer"},
			"default": "string",
			"description": "string: reply payload as text; " +
				"buffer: raw bytes",
		},
		"out": map[string]interface{}{
			"type":  "string",
			"title": "Close",
			"enum":  []string{"time", "char", "count", "sit", "immed"},
			"description": "Node-RED reply selector: time = after a " +
				"timeout of Splitc ms; char = after the Splitc character; " +
				"count = after Splitc characters; sit = never close " +
				"(stream replies); immed = immediately, don't wait",
		},
		"splitc": map[string]interface{}{
			"type":        "string",
			"title":       "Splitc",
			"description": "Node-RED close parameter for time/char/count: milliseconds, delimiter character or byte count",
		},
		"mode": map[string]interface{}{
			"type":  "string",
			"title": "Reply Framing (legacy)",
			"enum":  []string{"timeout", "delimiter", "length"},
			"description": "Legacy framing selector (timeout|delimiter|length); " +
				"ignored when Close (out) is set",
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
		"newline": map[string]interface{}{
			"type":        "string",
			"title":       "Reply Delimiter",
			"description": "In sit mode with a string return, replies split on this delimiter (escape sequences like \\n allowed)",
			"showIf":      map[string]interface{}{"out": "sit"},
		},
		"trim": map[string]interface{}{
			"type":        "boolean",
			"title":       "Re-attach Delimiter",
			"description": "Keep the delimiter attached to each reply part in sit mode",
			"default":     false,
			"showIf":      map[string]interface{}{"out": "sit"},
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
	// Server is the Node-RED host field ("" falls back to Host).
	Server string `json:"server"`
	// Host is the legacy host field (default 127.0.0.1).
	Host string `json:"host"`
	// Port is the remote port; message metadata "port" overrides it
	// (Node-RED msg.port semantics).
	Port int `json:"port"`
	// Ret selects the reply payload type: "string" (default) | "buffer"
	// (raw []byte). Node-RED field name: ret.
	Ret string `json:"ret"`
	// Out is the Node-RED close selector: "time" (reply after a
	// timeout of Splitc ms), "char" (reply ends at the Splitc
	// character), "count" (reply is exactly Splitc bytes), "sit"
	// (stay connected — stream replies) or "immed" (send and
	// close without waiting). "" falls back to the legacy Mode field.
	Out string `json:"out"`
	// Splitc is the Node-RED close parameter: milliseconds for
	// "time", the delimiter character for "char", the byte count for
	// "count". Serialized as a string by Node-RED.
	Splitc string `json:"splitc"`
	// Mode selects the reply framing: "timeout" (default — take
	// whatever arrives before Timeout), "delimiter" (read until the
	// delimiter bytes appear), "length" (read exactly Length bytes),
	// "sit" (stay connected, stream replies) or "immed" (don't wait
	// for a reply). The Node-RED Out field wins when set.
	Mode string `json:"mode"`
	// Timeout bounds connect + reply wait, in seconds (default 10;
	// 0 = 10). Out "time" overrides it with Splitc milliseconds.
	Timeout float64 `json:"timeout"`
	// Delimiter terminates the reply in "delimiter" mode
	// (escape sequences like \n allowed).
	Delimiter string `json:"delimiter"`
	// Length is the fixed reply size in "length" mode.
	Length int `json:"length"`
	// Newline delimits replies in "sit" mode when the return type is
	// string (escape sequences like \n allowed).
	Newline string `json:"newline"`
	// Trim re-attaches the delimiter to each reply part in "sit"
	// mode (Node-RED 're-attach' checkbox).
	Trim bool `json:"trim"`
	// Keep keeps the client connection open across messages instead of
	// closing it after each request/reply (Node-RED's persistent
	// client session). The connection is dropped on redeploy/stop.
	Keep bool `json:"keep"`
	// TLS names a tlsConfig node in the same workflow; when set the
	// connection is upgraded to TLS with that material.
	TLS string `json:"tls"`
}

// resolved carries the effective host/port/framing after folding the
// Node-RED fields (server/out/splitc) into the legacy ones.
type resolved struct {
	host      string
	port      int
	mode      string // timeout|delimiter|length|sit|immed
	timeout   float64
	delimiter string
	length    int
	keep      bool
}

// resolve folds the Node-RED serialized fields (server, out, splitc)
// over the engine-native ones, then applies message metadata
// overrides (Node-RED msg.host/msg.port → metadata host/port).
func (c *TCPRequestConfig) resolve(input nodestore.MessageInterface) resolved {
	r := resolved{
		host:      c.Host,
		port:      c.Port,
		mode:      c.Mode,
		timeout:   c.Timeout,
		delimiter: c.Delimiter,
		length:    c.Length,
		keep:      c.Keep,
	}
	if c.Server != "" {
		r.host = c.Server
	}
	if c.Out != "" {
		switch c.Out {
		case "char":
			r.mode = "delimiter"
			r.delimiter = unescapeSplitc(c.Splitc)
		case "count":
			r.mode = "length"
			r.length = int(floatOfString(c.Splitc))
		case "sit":
			r.mode = "sit"
			r.keep = true
		case "immed":
			r.mode = "immed"
		default: // "time"
			r.mode = "timeout"
			if ms := floatOfString(c.Splitc); ms > 0 {
				r.timeout = ms / 1000
			}
		}
	}
	if input != nil && input.GetMetadata() != nil {
		if v, ok := input.GetMetadata()["host"].(string); ok && v != "" {
			r.host = v
		}
		switch v := input.GetMetadata()["port"].(type) {
		case string:
			if p := floatOfString(v); p > 0 {
				r.port = int(p)
			}
		case float64:
			if v > 0 {
				r.port = int(v)
			}
		}
	}
	return r
}

// unescapeSplitc decodes the Node-RED "char" close parameter — escape
// spellings (\n, \r, \t, \e, \f, \0), a hex "0x.." byte or a raw
// character (first byte wins, mirroring charCodeAt(0)). Node-RED
// compares a single byte, so the result is one byte.
func unescapeSplitc(s string) string {
	var b byte
	if len(s) >= 2 && s[0] == '\\' {
		switch s[1] {
		case 'n':
			b = '\n'
		case 'r':
			b = '\r'
		case 't':
			b = '\t'
		case 'e':
			b = 0x1b
		case 'f':
			b = 0x0c
		case '0':
			b = 0x00
		default:
			b = s[0]
		}
		return string([]byte{b})
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		var v int64
		if _, err := fmt.Sscanf(s[2:], "%x", &v); err == nil {
			return string([]byte{byte(v)})
		}
	}
	if s == "" {
		return ""
	}
	return s[:1]
}

// floatOfString parses a Node-RED string-serialized number.
func floatOfString(s string) float64 {
	var f float64
	_, _ = fmt.Sscanf(strings.TrimSpace(s), "%g", &f)
	return f
}

// tcpRequestState is the per-node workflow state for keep-alive
// sessions — executors are rebuilt per message so the connection must
// live in host state. buffer carries an unsplit reply remainder across
// calls in sit mode (Node-RED's chunk accumulator).
type tcpRequestState struct {
	mu     sync.Mutex
	conn   net.Conn
	buffer []byte
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
	r := n.config.resolve(input)
	timeout := r.timeout
	if timeout <= 0 {
		timeout = 10
	}
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))

	var conn net.Conn
	var state *tcpRequestState
	if r.keep && n.host != nil {
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
		conn, err = n.dial(r, deadline)
		if err != nil {
			return nil, err
		}
	}
	if state == nil {
		defer func() { _ = conn.Close() }()
	} else {
		state.conn = conn
	}

	if err := conn.SetWriteDeadline(deadline); err != nil {
		return nil, fmt.Errorf("tcpRequest write deadline failed: %w", err)
	}
	remote := ""
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
	}
	if _, err := conn.Write(data); err != nil {
		n.drop(state)
		return nil, fmt.Errorf("tcpRequest write to %s failed: %w", remote, err)
	}

	if r.mode == "immed" {
		// "immed" (Node-RED): send and close without waiting — the
		// request carries no reply.
		n.drop(state)
		return input, nil
	}

	reply, err := n.readReply(conn, deadline, r, state)
	if err != nil {
		n.drop(state)
		return nil, fmt.Errorf("tcpRequest read from %s failed: %w", remote, err)
	}
	if reply == nil {
		// sit mode: the buffered remainder carries no complete part
		// yet — pass the message through unchanged.
		return input, nil
	}

	metadata := nodebase.CloneMetadata(input)
	if host, port, serr := net.SplitHostPort(remote); serr == nil {
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
func (n *TCPRequestNode) dial(r resolved, deadline time.Time) (net.Conn, error) {
	host := r.host
	if host == "" {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", r.port))
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

// readReply consumes the server reply per the configured framing. A
// nil reply in "sit" mode means the buffered remainder holds no
// complete part yet (the message passes through unchanged).
func (n *TCPRequestNode) readReply(conn net.Conn, deadline time.Time, r resolved, state *tcpRequestState) ([]byte, error) {
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	switch r.mode {
	case "", "timeout":
		// Whatever arrives before the deadline is the reply; a
		// half-close (EOF) ends early.
		return io.ReadAll(conn)
	case "delimiter":
		delim := nodebase.UnescapeTCP(r.delimiter)
		if delim == "" {
			return nil, errors.New("tcpRequest delimiter mode requires a delimiter")
		}
		return readUntil(conn, []byte(delim), deadline)
	case "length":
		if r.length <= 0 {
			return nil, errors.New("tcpRequest length mode requires a positive length")
		}
		buf := make([]byte, r.length)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return nil, err
		}
		return buf, nil
	case "sit":
		// Stay connected; each read window returns one reply. With a
		// string return and a configured delimiter, replies split on
		// it and the remainder carries into the next call.
		return n.readSit(conn, deadline, r, state)
	default:
		return nil, fmt.Errorf("invalid tcpRequest mode %q (timeout|delimiter|length|sit|immed)", r.mode)
	}
}

// readSit reads one reply chunk from a kept "sit" connection. The
// deadline bounds this call only — the connection stays open.
func (n *TCPRequestNode) readSit(conn net.Conn, deadline time.Time, r resolved, state *tcpRequestState) ([]byte, error) {
	delim := ""
	if n.config.Ret == "string" || n.config.Ret == "" {
		delim = nodebase.UnescapeTCP(n.config.Newline)
	}
	tmp := make([]byte, 4096)
	for {
		if delim != "" && state != nil {
			if idx := bytes.Index(state.buffer, []byte(delim)); idx >= 0 {
				end := idx
				if n.config.Trim {
					end = idx + len(delim)
				}
				part := append([]byte(nil), state.buffer[:end]...)
				state.buffer = state.buffer[idx+len(delim):]
				return part, nil
			}
		}
		_ = conn.SetReadDeadline(deadline)
		nr, err := conn.Read(tmp)
		if nr > 0 {
			if state != nil {
				state.buffer = append(state.buffer, tmp[:nr]...)
			}
			if delim == "" {
				// No delimiter — each received chunk is a reply.
				return append([]byte(nil), tmp[:nr]...), nil
			}
			continue // re-check the buffer for a completed part
		}
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				// Quiet window: flush whatever was buffered, if any.
				if state != nil && len(state.buffer) > 0 {
					out := state.buffer
					state.buffer = nil
					return out, nil
				}
				return nil, nil
			}
			return nil, err
		}
	}
}

// readUntil reads until delim appears (and includes it) or the deadline
// trips. io.ReadAll on a deadline cannot do this directly.
func readUntil(conn net.Conn, delim []byte, deadline time.Time) ([]byte, error) {
	buf := make([]byte, 0, 4096)
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
	r := n.config.resolve(nil)
	switch r.mode {
	case "", "timeout", "sit", "immed":
	case "delimiter":
		if nodebase.UnescapeTCP(r.delimiter) == "" {
			return errors.New("tcpRequest delimiter mode requires a delimiter")
		}
	case "length":
		if r.length <= 0 {
			return errors.New("tcpRequest length mode requires a positive length")
		}
	default:
		return fmt.Errorf("invalid tcpRequest mode %q (timeout|delimiter|length|sit|immed)", r.mode)
	}
	switch n.config.Out {
	case "", "time", "char", "count", "sit", "immed":
	default:
		return fmt.Errorf("invalid tcpRequest out %q (time|char|count|sit|immed)", n.config.Out)
	}
	switch n.config.Ret {
	case "", "string", "buffer":
	default:
		return fmt.Errorf("invalid tcpRequest ret %q (string|buffer)", n.config.Ret)
	}
	return nil
}
