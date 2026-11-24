// Package tcpin contains the tcpIn node implementation.
package tcpin

import (
	"bufio"
	"context"
	"crypto/tls"
	_ "embed"
	"errors"
	"fmt"
	"net"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
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
		"mode": map[string]interface{}{
			"type":    "string",
			"title":   "Mode",
			"enum":    []string{"listen", "connect"},
			"default": "listen",
			"description": "listen: accept connections on port; " +
				"connect: dial out to host:port",
		},
		"host": map[string]string{
			"type":        "string",
			"title":       "Host",
			"description": "Remote host for connect mode (default 127.0.0.1)",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "Listen port (listen mode) or remote port (connect mode)",
		},
	},
	"required": []string{"port"},
}

// TCPInConfig holds configuration for the tcpIn node.
type TCPInConfig struct {
	// Mode selects "listen" (accept connections on port) or
	// "connect" (dial out to host:port).
	Mode string `json:"mode"`
	// Host is the remote host for connect mode (default 127.0.0.1).
	Host string `json:"host"`
	// Port is the listen port (listen mode) or remote port (connect).
	Port int `json:"port"`
	// TLS names a tlsConfig node in the same workflow; in connect
	// mode the dialed connection is upgraded to TLS with that material.
	TLS string `json:"tls"`
}

// TCPInNode receives raw TCP data — either by listening for inbound
// connections or by connecting to a remote host. Each received line
// (bufio scanner) is emitted downstream as Data with the remote
// ip/port in Metadata. Accepted connections are registered in the
// workflow's shared tcpSessions map for tcpOut reply mode.
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

	switch n.config.Mode {
	case "", "listen":
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.config.Port))
		if err != nil {
			return nil, fmt.Errorf("tcpIn cannot listen on :%d: %w", n.config.Port, err)
		}
		n.listener = listener
		go n.acceptLoop()
		return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening", "port": n.config.Port}).SetTimestamp(time.Now()), nil
	case "connect":
		host := n.config.Host
		if host == "" {
			host = "127.0.0.1"
		}
		conn, err := n.dial(host)
		if err != nil {
			return nil, err
		}
		n.conn = conn
		go n.readLoop(conn)
		return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "connected", "host": host, "port": n.config.Port}).SetTimestamp(time.Now()), nil
	default:
		return nil, fmt.Errorf("invalid tcpIn mode %q (listen|connect)", n.config.Mode)
	}
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
		if cfg.ServerName == "" {
			cfg = cfg.Clone()
			cfg.ServerName = host
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
		n.sess.Put(conn.RemoteAddr().String(), conn)
		go n.readLoop(conn)
	}
}

// readLoop scans lines from a connection and emits each one downstream
// via the host's fire-and-forget emit path. On close the connection
// is removed from the session map.
func (n *TCPInNode) readLoop(conn net.Conn) {
	defer func() {
		key := conn.RemoteAddr().String()
		n.sess.DeleteIf(key, conn)
		_ = conn.Close()
	}()

	host, portStr, _ := net.SplitHostPort(conn.RemoteAddr().String())
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		payload := nodestore.NewMessage().SetPayload(scanner.Text()).SetMetadata(map[string]interface{}{
			"ip":   host,
			"port": portStr,
		}).SetTimestamp(time.Now())
		_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
	}
	// net.ErrClosed is the normal Stop() path; anything else is a real
	// read error worth surfacing.
	if err := scanner.Err(); err != nil && !errors.Is(err, net.ErrClosed) {
		n.host.EmitEvent(n.workflowID, n.GetID(), "tcpIn", "warn", "tcp read error: "+err.Error())
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
	switch n.config.Mode {
	case "", "listen", "connect":
		return nil
	default:
		return fmt.Errorf("invalid tcpIn mode %q (listen|connect)", n.config.Mode)
	}
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
