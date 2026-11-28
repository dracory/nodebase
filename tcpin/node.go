// Package tcpin contains the tcpIn node implementation.
package tcpin

import (
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/base64"
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
	c := new(TCPInConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewTCPInNode(n, c, e), nil
}

// init registers the tcpIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "tcpIn",
			Name:        "TCP In",
			Description: "Receives raw TCP data (listen or connect)",
			Icon:        "network-wired",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the tcpIn node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"server": map[string]interface{}{
			"type":    "string",
			"title":   "Type",
			"enum":    []string{"server", "client"},
			"default": "server",
			"description": "server: accept connections on port; " +
				"client: dial out to host:port (Node-RED 'server' field)",
		},
		"mode": map[string]interface{}{
			"type":  "string",
			"title": "Mode",
			"enum":  []string{"listen", "connect"},
			"description": "Legacy alias of Type (listen=server, " +
				"connect=client); ignored when Type is set",
		},
		"host": map[string]interface{}{
			"type":        "string",
			"title":       "Host",
			"description": "Remote host for connect mode (default 127.0.0.1)",
			"showIf":      map[string]interface{}{"server": "client"},
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "Listen port (listen mode) or remote port (connect mode)",
		},
		"datamode": map[string]interface{}{
			"type":    "string",
			"title":   "Output",
			"enum":    []string{"stream", "single", "length", "timeout"},
			"default": "stream",
			"description": "stream: emit each chunk / delimited part; " +
				"single: one message with the whole buffer at close; " +
				"length: emit every Length bytes; " +
				"timeout: flush the buffer after FlushMS ms of silence",
		},
		"datatype": map[string]interface{}{
			"type":    "string",
			"title":   "Payload",
			"enum":    []string{"utf8", "buffer", "base64"},
			"default": "utf8",
			"description": "utf8: string payload; buffer: raw bytes; " +
				"base64: base64-encoded string",
		},
		"newline": map[string]interface{}{
			"type":        "string",
			"title":       "Delimiter",
			"description": "Stream delimiter for utf8 payloads (escape sequences like \\n allowed; empty = emit each chunk)",
			"showIf":      map[string]interface{}{"datamode": "stream", "datatype": "utf8"},
		},
		"trim": map[string]interface{}{
			"type":        "boolean",
			"title":       "Re-attach Delimiter",
			"description": "Keep the delimiter attached to each emitted part",
			"default":     false,
			"showIf":      map[string]interface{}{"datamode": "stream", "datatype": "utf8"},
		},
		"length": map[string]interface{}{
			"type":        "integer",
			"title":       "Length",
			"description": "Chunk size for length mode",
			"showIf":      map[string]interface{}{"datamode": "length"},
		},
		"flushms": map[string]interface{}{
			"type":        "integer",
			"title":       "Flush (ms)",
			"description": "Idle interval before the buffered data flushes in timeout mode",
			"showIf":      map[string]interface{}{"datamode": "timeout"},
		},
		"topic": map[string]interface{}{
			"type":        "string",
			"title":       "Topic",
			"description": "Stamped on emitted messages as metadata topic",
		},
		"tls": map[string]interface{}{
			"type":        "string",
			"format":      "nodeselect",
			"peerType":    "tlsConfig",
			"title":       "TLS Config",
			"description": "Optional tlsConfig node; connect-mode dials are upgraded to TLS with that material",
			"showIf":      map[string]interface{}{"server": "client"},
		},
	},
	"required": []string{"port"},
}

// TCPInConfig holds configuration for the tcpIn node.
type TCPInConfig struct {
	// Mode selects "listen" (accept connections on port) or
	// "connect" (dial out to host:port) — legacy alias of the
	// Node-RED "server" field; Server wins when set.
	Mode string `json:"mode"`
	// Server is the Node-RED connection selector: "server" listens
	// on Port, "client" dials Host:Port.
	Server string `json:"server"`
	// Host is the remote host for connect mode (default 127.0.0.1).
	Host string `json:"host"`
	// Port is the listen port (listen mode) or remote port (connect).
	Port int `json:"port"`
	// Datamode selects the output framing: "stream" (default —
	// emit each chunk, or each delimited part for utf8 with a
	// delimiter), "single" (one message with the whole buffer at
	// connection close), "length" (emit every Length bytes) or
	// "timeout" (flush the buffered data after FlushMS ms idle).
	Datamode string `json:"datamode"`
	// Datatype selects the payload type: "utf8" (string — default,
	// matching the previous always-string behavior), "buffer"
	// (raw []byte) or "base64" (base64-encoded string).
	Datatype string `json:"datatype"`
	// Newline is the stream delimiter for utf8 payloads — a pointer
	// so a blank-but-explicit "" (Node-RED 'no delimiter — emit each
	// chunk') is distinct from unset (legacy "\n" line mode).
	Newline *string `json:"newline"`
	// Trim re-attaches the delimiter to each emitted part
	// (Node-RED 're-attach' checkbox).
	Trim bool `json:"trim"`
	// Topic is stamped on emitted messages as metadata "topic".
	Topic string `json:"topic"`
	// Length is the fixed chunk size for datamode "length".
	Length int `json:"length"`
	// FlushMS is the idle flush interval (milliseconds) for
	// datamode "timeout".
	FlushMS int `json:"flushms"`
	// Base64 is a deprecated Node-RED field — decoded, unused.
	Base64 bool `json:"base64"`
	// TLS names a tlsConfig node in the same workflow; in connect
	// mode the dialed connection is upgraded to TLS with that material.
	TLS string `json:"tls"`
}

// isConnect resolves the connection direction: the Node-RED "server"
// field wins; the legacy Mode field is the fallback.
func (c *TCPInConfig) isConnect() bool {
	switch c.Server {
	case "client":
		return true
	case "server":
		return false
	}
	return c.Mode == "connect"
}

// datamode resolves the framing mode (default "stream").
func (c *TCPInConfig) datamode() string {
	if c.Datamode == "" {
		return "stream"
	}
	return c.Datamode
}

// datatype resolves the payload type (default "utf8" — the previous
// always-string behavior).
func (c *TCPInConfig) datatype() string {
	if c.Datatype == "" {
		return "utf8"
	}
	return c.Datatype
}

// newline resolves the utf8 stream delimiter: unset means the legacy
// "\n" line mode; a configured value unescapes (\n, \r, \t, \0) and
// may be deliberately empty ("emit each chunk").
func (c *TCPInConfig) newline() string {
	if c.Newline == nil {
		return "\n"
	}
	return nodebase.UnescapeTCP(*c.Newline)
}

// TCPInNode receives raw TCP data — either by listening for inbound
// connections or by connecting to a remote host. Received data is
// framed per the datamode (stream/single/length/timeout) and emitted
// downstream with the remote ip/port in Metadata. Accepted connections
// are registered in the workflow's shared tcpSessions map for tcpOut
// reply mode.
type TCPInNode struct {
	*nodebase.BaseNode
	config     *TCPInConfig
	host       nodebase.NodeHost
	workflowID string
	listener   net.Listener
	conn       net.Conn // connect mode dial
	stopOnce   sync.Once
	// sess caches the workflow session registry resolved in Execute —
	// Stop must not call GetOrCreateNodeState because StopWorkflow
	// holds the host lock while invoking stoppers.
	sess *nodebase.TCPSessions
}

// Execute activates the node: listen mode binds a socket and spawns an
// accept loop; connect mode dials and starts one reader goroutine.
func (n *TCPInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	// Resolve the shared session registry up front: host state is
	// unreachable inside Stop (StopWorkflow holds the host lock).
	n.sess = n.sessions()

	if !n.config.isConnect() {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.config.Port))
		if err != nil {
			return nil, fmt.Errorf("tcpIn cannot listen on :%d: %w", n.config.Port, err)
		}
		n.listener = listener
		go n.acceptLoop()
		return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening", "port": n.config.Port}).SetTimestamp(time.Now()), nil
	}
	host := n.config.Host
	if host == "" {
		host = "127.0.0.1"
	}
	conn, err := n.dialRetry(host)
	if err != nil {
		return nil, err
	}
	n.conn = conn
	go n.readLoop(conn)
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "connected", "host": host, "port": n.config.Port}).SetTimestamp(time.Now()), nil
}

// dialRetry attempts the connect-mode dial with a short retry window:
// Node-RED reconnects in the background, so a peer that binds a moment
// later (e.g. a tcpOut listener activated next in the same deploy)
// must not fail the activation.
func (n *TCPInNode) dialRetry(host string) (net.Conn, error) {
	deadline := time.Now().Add(2 * time.Second)
	conn, err := n.dial(host)
	for err != nil && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		conn, err = n.dial(host)
	}
	return conn, err
}

// dial opens the connect-mode connection, upgraded to TLS when the
// node's "tls" field references a tlsConfig node.
func (n *TCPInNode) dial(host string) (net.Conn, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", n.config.Port))
	if n.config.TLS != "" {
		cfg, err := n.host.LoadTLSConfig(n.workflowID, n.config.TLS)
		if err != nil {
			return nil, err
		}
		if cfg == nil {
			return nil, fmt.Errorf("tcpIn tls config %q resolved empty", n.config.TLS)
		}
		if cfg.ServerName == "" {
			cloned := cfg.Clone()
			if cloned == nil {
				return nil, fmt.Errorf("tcpIn tls config %q could not be cloned", n.config.TLS)
			}
			cloned.ServerName = host
			cfg = cloned
		}
		return tls.Dial("tcp", addr, cfg)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcpIn cannot connect to %s:%d: %w", host, n.config.Port, err)
	}
	return conn, nil
}

// acceptLoop accepts connections until the listener is closed, spawning
// a reader per connection and registering it in the session map.
func (n *TCPInNode) acceptLoop() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			return // listener closed (Stop) or fatal — either way we exit
		}
		remote := conn.RemoteAddr()
		if remote == nil {
			_ = conn.Close()
			continue
		}
		n.sess.Put(remote.String(), conn)
		go n.readLoop(conn)
	}
}

// payload converts a received byte slice into the configured output
// type (utf8 string, raw buffer, or base64 string).
func (n *TCPInNode) payload(b []byte) interface{} {
	switch n.config.datatype() {
	case "buffer":
		return b
	case "base64":
		return base64.StdEncoding.EncodeToString(b)
	default:
		return string(b)
	}
}

// readLoop frames received data per the configured datamode and emits
// each piece downstream via the host's fire-and-forget emit path. On
// close the connection is removed from the session map.
func (n *TCPInNode) readLoop(conn net.Conn) {
	remote := ""
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
	}
	defer func() {
		n.sess.DeleteIf(remote, conn)
		_ = conn.Close()
	}()

	host, portStr, _ := net.SplitHostPort(remote)
	emit := func(data []byte) {
		metadata := map[string]interface{}{
			"ip":   host,
			"port": portStr,
		}
		if n.config.Topic != "" {
			metadata["topic"] = n.config.Topic
		}
		payload := nodestore.NewMessage().SetPayload(n.payload(data)).SetMetadata(metadata).SetTimestamp(time.Now())
		_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
	}
	readErr := n.frame(conn, emit)
	// net.ErrClosed is the normal Stop() path; anything else is a real
	// read error worth surfacing.
	if readErr != nil && !errors.Is(readErr, net.ErrClosed) && !errors.Is(readErr, io.EOF) {
		n.host.EmitEvent(n.workflowID, n.GetID(), "tcpIn", "warn", "tcp read error: "+readErr.Error())
	}
}

// frame drives the connection read loop for the configured datamode,
// calling emit for each completed piece and returning the terminal
// read error (io.EOF on a clean close).
func (n *TCPInNode) frame(conn net.Conn, emit func([]byte)) error {
	switch n.config.datamode() {
	case "single":
		// One message with everything the connection delivered.
		buf, err := io.ReadAll(conn)
		if len(buf) > 0 {
			emit(buf)
		}
		return err
	case "length":
		return n.frameLength(conn, emit)
	case "timeout":
		return n.frameTimeout(conn, emit)
	default:
		return n.frameStream(conn, emit)
	}
}

// frameStream implements datamode "stream": buffer/base64 payloads
// (or utf8 with an empty delimiter) emit every received chunk; utf8
// with a delimiter accumulates and emits each delimited part.
func (n *TCPInNode) frameStream(conn net.Conn, emit func([]byte)) error {
	delim := ""
	if n.config.datatype() == "utf8" {
		delim = n.config.newline()
	}
	var buf []byte
	tmp := make([]byte, 64*1024)
	for {
		nr, err := conn.Read(tmp)
		buf = append(buf, tmp[:nr]...)
		if delim == "" {
			if nr > 0 {
				emit(append([]byte(nil), buf[:nr]...))
			}
			buf = buf[:0]
		} else {
			for {
				idx := strings.Index(string(buf), delim)
				if idx < 0 {
					break
				}
				part := buf[:idx]
				if n.config.Trim {
					part = buf[:idx+len(delim)]
				} else if delim == "\n" && len(part) > 0 && part[len(part)-1] == '\r' {
					// Mirror bufio.ScanLines (the legacy framing):
					// a "\r\n" line ending loses the '\r'.
					part = part[:len(part)-1]
				}
				emit(append([]byte(nil), part...))
				buf = buf[idx+len(delim):]
			}
		}
		if err != nil {
			if len(buf) > 0 && !errors.Is(err, net.ErrClosed) {
				// Flush the unterminated remainder at close, as
				// Node-RED does on 'end'.
				emit(buf)
			}
			return err
		}
	}
}

// frameLength implements datamode "length": emit every Length bytes;
// any remainder flushes at close.
func (n *TCPInNode) frameLength(conn net.Conn, emit func([]byte)) error {
	size := n.config.Length
	var buf []byte
	tmp := make([]byte, 64*1024)
	for {
		nr, err := conn.Read(tmp)
		buf = append(buf, tmp[:nr]...)
		for len(buf) >= size {
			emit(append([]byte(nil), buf[:size]...))
			buf = buf[size:]
		}
		if err != nil {
			if len(buf) > 0 && !errors.Is(err, net.ErrClosed) {
				emit(buf)
			}
			return err
		}
	}
}

// frameTimeout implements datamode "timeout": accumulate data and
// flush once the connection has been idle for FlushMS milliseconds;
// any remainder flushes at close.
func (n *TCPInNode) frameTimeout(conn net.Conn, emit func([]byte)) error {
	idle := time.Duration(n.config.FlushMS) * time.Millisecond
	var buf []byte
	tmp := make([]byte, 64*1024)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(idle))
		nr, err := conn.Read(tmp)
		buf = append(buf, tmp[:nr]...)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				if len(buf) > 0 {
					emit(buf)
					buf = nil
				}
				continue
			}
			if len(buf) > 0 && !errors.Is(err, net.ErrClosed) {
				emit(buf)
			}
			return err
		}
	}
}

// sessions returns (creating if needed) the workflow-level connection
// registry shared with tcpOut.
func (n *TCPInNode) sessions() *nodebase.TCPSessions {
	return n.host.GetOrCreateNodeState(n.workflowID, nodebase.TCPSessionsKey, func() interface{} {
		return nodebase.NewTCPSessions()
	}).(*nodebase.TCPSessions)
}

// Stop shuts down the listener / outbound connection and every
// accepted session registered by this workflow's tcpIn nodebase.
func (n *TCPInNode) Stop() error {
	var err error
	n.stopOnce.Do(func() {
		if n.listener != nil {
			err = n.listener.Close()
		}
		if n.conn != nil {
			_ = n.conn.Close()
		}
		if n.sess != nil {
			n.sess.CloseAll()
		}
	})
	return err
}

// Validate checks the tcpIn node configuration
func (n *TCPInNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Port <= 0 || n.config.Port > 65535 {
		return fmt.Errorf("tcpIn requires a valid port (1-65535)")
	}
	switch n.config.Server {
	case "", "server", "client":
	default:
		return fmt.Errorf("invalid tcpIn type %q (server|client)", n.config.Server)
	}
	if n.config.Server == "" {
		switch n.config.Mode {
		case "", "listen", "connect":
		default:
			return fmt.Errorf("invalid tcpIn mode %q (listen|connect)", n.config.Mode)
		}
	}
	switch n.config.datamode() {
	case "stream", "single":
	case "length":
		if n.config.Length <= 0 {
			return errors.New("tcpIn length mode requires a positive length")
		}
	case "timeout":
		if n.config.FlushMS <= 0 {
			return errors.New("tcpIn timeout mode requires a positive flush interval")
		}
	default:
		return fmt.Errorf("invalid tcpIn datamode %q (stream|single|length|timeout)", n.config.Datamode)
	}
	switch n.config.datatype() {
	case "utf8", "buffer", "base64":
	default:
		return fmt.Errorf("invalid tcpIn datatype %q (utf8|buffer|base64)", n.config.Datatype)
	}
	return nil
}

// NewTCPInNode creates a new tcpIn node
func NewTCPInNode(node nodestore.NodeInterface, config *TCPInConfig, host nodebase.NodeHost) *TCPInNode {
	return &TCPInNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute activates the node: listen mode binds a socket and spawns an
// accept loop; connect mode dials and starts one reader goroutine.
