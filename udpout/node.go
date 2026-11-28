// Package udpout contains the udpOut node implementation.
package udpout

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"net"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(UDPOutConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewUDPOutNode(n, c), nil
}

// init registers the udpOut node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "udpOut",
			Name:        "UDP Out",
			Description: "Sends a UDP datagram",
			Icon:        "share",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the udpOut node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"multicast": map[string]interface{}{
			"type":    "string",
			"title":   "Send",
			"enum":    []string{"false", "broad", "multi"},
			"default": "false",
			"description": "false: a UDP datagram; broad: a broadcast " +
				"message; multi: a multicast message to the group address",
		},
		"addr": map[string]interface{}{
			"type":        "string",
			"title":       "Address",
			"description": "Target address (default 127.0.0.1; broadcast address for broad, group for multi); the message's ip metadata overrides it",
		},
		"host": map[string]string{
			"type":        "string",
			"title":       "Host",
			"description": "Legacy alias of Address; ignored when Address is set",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "Target port; the message's port metadata overrides it",
		},
		"ipv": map[string]interface{}{
			"type":    "string",
			"title":   "Using",
			"enum":    []string{"udp4", "udp6"},
			"default": "udp4",
		},
		"outport": map[string]interface{}{
			"type":        "integer",
			"title":       "Local Port",
			"description": "Bind the local port instead of using a random one",
		},
		"base64": map[string]interface{}{
			"type":        "boolean",
			"title":       "Decode Base64",
			"description": "Decode a base64 payload string before sending",
			"default":     false,
		},
	},
}

// UDPOutConfig holds configuration for the udpOut node.
type UDPOutConfig struct {
	// Addr is the Node-RED target address field; overridden by
	// message Metadata["ip"] when present. Host is the legacy alias.
	Addr string `json:"addr"`
	// Host is the legacy target field (default 127.0.0.1).
	Host string `json:"host"`
	// Port is the target port; overridden by message
	// Metadata["port"] when present.
	Port int `json:"port"`
	// IPv selects the socket family: "udp4" (default) | "udp6".
	IPv string `json:"ipv"`
	// Outport binds a fixed local port; 0 picks a random one.
	Outport int `json:"outport"`
	// Base64 decodes a base64 payload string before sending.
	Base64 bool `json:"base64"`
	// Multicast selects the send flavor: "false" (datagram),
	// "broad" (broadcast — defaults the address to 255.255.255.255)
	// or "multi" (multicast to the group address).
	Multicast string `json:"multicast"`
	// Iface names the local interface for multicast sends.
	Iface string `json:"iface"`
}

// UDPOutNode sends the message Data as a UDP datagram to the
// configured host:port, or to the message's Metadata ip/port when
// present (e.g. answering a udpIn sender).
type UDPOutNode struct {
	*nodebase.BaseNode
	config *UDPOutConfig
}

// Execute resolves the target address (metadata wins) and sends one
// datagram; the input passes through unchanged.
func (n *UDPOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	data, err := nodebase.TCPPayloadBytes(input)
	if err != nil {
		return nil, err
	}
	if n.config.Base64 {
		if s, ok := input.GetPayload().(string); ok {
			raw, derr := base64.StdEncoding.DecodeString(s)
			if derr != nil {
				return nil, fmt.Errorf("udpOut cannot decode base64 payload: %w", derr)
			}
			data = raw
		}
	}

	host := n.config.Addr
	if host == "" {
		host = n.config.Host
	}
	port := n.config.Port
	if input != nil && input.GetMetadata() != nil {
		if v, ok := input.GetMetadata()["ip"].(string); ok && v != "" {
			host = v
		}
		switch v := input.GetMetadata()["port"].(type) {
		case string:
			var p int
			if _, err := fmt.Sscanf(v, "%d", &p); err == nil && p > 0 {
				port = p
			}
		case float64:
			if v > 0 {
				port = int(v)
			}
		}
	}
	if host == "" {
		if n.config.Multicast == "broad" {
			host = "255.255.255.255"
		} else {
			host = "127.0.0.1"
		}
	}
	if port <= 0 {
		return nil, fmt.Errorf("udpOut requires a port (config or message metadata)")
	}

	network := n.config.IPv
	if network == "" {
		network = "udp4"
	}
	var laddr *net.UDPAddr
	if n.config.Outport > 0 {
		laddr = &net.UDPAddr{Port: n.config.Outport}
	}
	target, err := net.ResolveUDPAddr(network, net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		return nil, fmt.Errorf("udpOut invalid target address %q: %w", host, err)
	}
	conn, err := net.DialUDP(network, laddr, target)
	if err != nil {
		return nil, fmt.Errorf("udpOut cannot reach %s:%d: %w", host, port, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("udpOut write failed: %w", err)
	}
	return input, nil
}

// Validate checks the udpOut node configuration
func (n *UDPOutNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	switch n.config.Multicast {
	case "", "false", "broad", "multi":
	default:
		return fmt.Errorf("invalid udpOut multicast %q (false|broad|multi)", n.config.Multicast)
	}
	switch n.config.IPv {
	case "", "udp4", "udp6":
	default:
		return fmt.Errorf("invalid udpOut ipv %q (udp4|udp6)", n.config.IPv)
	}
	return nil
}

// NewUDPOutNode creates a new udpOut node
func NewUDPOutNode(node nodestore.NodeInterface, config *UDPOutConfig) *UDPOutNode {
	return &UDPOutNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute resolves the target address (metadata wins) and sends one
// datagram; the input passes through unchanged.
