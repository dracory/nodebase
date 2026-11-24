// Package udpout contains the udpOut node implementation.
package udpout

import (
	"context"
	_ "embed"
	"fmt"
	"net"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
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
		"host": map[string]string{
			"type":        "string",
			"title":       "Host",
			"description": "Target host (default 127.0.0.1); the message's ip metadata overrides it",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "Target port; the message's port metadata overrides it",
		},
	},
}

// UDPOutConfig holds configuration for the udpOut node.
type UDPOutConfig struct {
	// Host is the target host (default 127.0.0.1); overridden by
	// message Metadata["ip"] when present.
	Host string `json:"host"`
	// Port is the target port; overridden by message
	// Metadata["port"] when present.
	Port int `json:"port"`
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

	host := n.config.Host
	port := n.config.Port
	if input != nil && input.GetMetadata() != nil {
		if v, ok := input.GetMetadata()["ip"].(string); ok && v != "" {
			host = v
		}
		if v, ok := input.GetMetadata()["port"].(string); ok && v != "" {
			var p int
			if _, err := fmt.Sscanf(v, "%d", &p); err == nil && p > 0 {
				port = p
			}
		}
	}
	if host == "" {
		host = "127.0.0.1"
	}
	if port <= 0 {
		return nil, fmt.Errorf("udpOut requires a port (config or message metadata)")
	}

	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP(host), Port: port})
	if err != nil {
		return nil, fmt.Errorf("udpOut cannot reach %s:%d: %w", host, port, err)
	}
	defer conn.Close()
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("udpOut write failed: %w", err)
	}
	return input, nil
}

// Validate checks the udpOut node configuration
func (n *UDPOutNode) Validate() error {
	return n.BaseNode.Validate()
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
