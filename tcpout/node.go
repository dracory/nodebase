// Package tcpout contains the tcpOut node implementation.
package tcpout

import (
	"context"
	"crypto/tls"
	_ "embed"
	"errors"
	"fmt"
	"net"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"time"
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
		"host": map[string]string{
			"type":        "string",
			"title":       "Host",
			"description": "Target host (default 127.0.0.1)",
		},
		"port": map[string]interface{}{
			"type":  "integer",
			"title": "Port",
		},
		"reply": map[string]interface{}{
			"type":        "boolean",
			"title":       "Reply to Session",
			"description": "Write back on the tcpIn connection that produced this message (uses its ip/port metadata)",
			"default":     false,
		},
	},
}

// TCPOutConfig holds configuration for the tcpOut node.
type TCPOutConfig struct {
	// Host is the target host (default 127.0.0.1) for non-reply mode.
	Host string `json:"host"`
	// Port is the target port for non-reply mode.
	Port int `json:"port"`
	// Reply writes back on the tcpIn session that produced the message
	// (looked up via Metadata ip/port) instead of dialing host:port.
	Reply bool `json:"reply"`
	// TLS names a tlsConfig node in the same workflow; non-reply mode
	// dials are upgraded to TLS with that material.
	TLS string `json:"tls"`
}

// TCPOutNode writes the message Data over TCP — either by replying on
// the accepted connection that produced the message (reply mode) or by
// dialing a configured host:port (open-write-close).
type TCPOutNode struct {
	*nodebase.BaseNode
	config     *TCPOutConfig
	host       nodebase.NodeHost
	workflowID string
}

// Execute sends the payload's Data and returns the input unchanged.
func (n *TCPOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	data, err := nodebase.TCPPayloadBytes(input)
	if err != nil {
		return nil, err
	}

	if n.config.Reply {
		return n.reply(input, data)
	}

	host := n.config.Host
	if host == "" {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", n.config.Port))
	var conn net.Conn
	if n.config.TLS != "" {
		cfg, terr := n.host.LoadTLSConfig(n.workflowID, n.config.TLS)
		if terr != nil {
			return nil, terr
		}
		if cfg.ServerName == "" {
			cfg = cfg.Clone()
			cfg.ServerName = host
		}
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, cfg)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 10*time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("tcpOut cannot connect to %s: %w", addr, err)
	}
	defer conn.Close()
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("tcpOut write failed: %w", err)
	}
	return input, nil
}

// reply writes to the accepted tcpIn connection identified by the
// message's Metadata ip/port (emitted by a tcpIn listen node).
func (n *TCPOutNode) reply(input nodestore.MessageInterface, data []byte) (nodestore.MessageInterface, error) {
	if input == nil || input.GetMetadata() == nil {
		return nil, errors.New("tcpOut reply mode requires message ip/port metadata")
	}
	ip, _ := input.GetMetadata()["ip"].(string)
	port, _ := input.GetMetadata()["port"].(string)
	if ip == "" || port == "" {
		return nil, errors.New("tcpOut reply mode requires message ip/port metadata")
	}
	key := net.JoinHostPort(ip, port)

	sessions := n.host.GetOrCreateNodeState(n.workflowID, nodebase.TCPSessionsKey, func() interface{} {
		return nodebase.NewTCPSessions()
	}).(*nodebase.TCPSessions)
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
	if !n.config.Reply && (n.config.Port <= 0 || n.config.Port > 65535) {
		return fmt.Errorf("tcpOut requires a valid port (1-65535) unless reply mode is set")
	}
	return nil
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

// Execute sends the payload's Data and returns the input unchanged.
