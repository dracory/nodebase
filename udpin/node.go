// Package udpin contains the udpIn node implementation.
package udpin

import (
	"context"
	_ "embed"
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
	c := new(UDPInConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewUDPInNode(n, c, e), nil
}

// init registers the udpIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "udpIn",
			Name:        "UDP In",
			Description: "Receives UDP datagrams",
			Icon:        "satellite-dish",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the udpIn node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "UDP port to listen on",
		},
	},
	"required": []string{"port"},
}

// UDPInConfig holds configuration for the udpIn node.
type UDPInConfig struct {
	// Port is the UDP port to listen on.
	Port int `json:"port"`
}

// UDPInNode receives UDP datagrams. Each datagram is emitted
// downstream as Data (string) with the sender's ip/port in Metadata.
type UDPInNode struct {
	*nodebase.BaseNode
	config     *UDPInConfig
	host       nodebase.NodeHost
	workflowID string
	conn       *net.UDPConn
	stopOnce   sync.Once
}

// Execute binds the UDP socket and starts the datagram read loop.
func (n *UDPInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: n.config.Port})
	if err != nil {
		return nil, fmt.Errorf("udpIn cannot listen on :%d: %w", n.config.Port, err)
	}
	n.conn = conn
	go n.readLoop()
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening", "port": n.config.Port}).SetTimestamp(time.Now()), nil
}

// readLoop emits each received datagram downstream until the socket
// is closed.
func (n *UDPInNode) readLoop() {
	buf := make([]byte, 64*1024)
	for {
		nr, addr, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			return // socket closed (Stop)
		}
		payload := nodestore.NewMessage().SetPayload(string(buf[:nr])).SetMetadata(map[string]interface{}{
			"ip":   addr.IP.String(),
			"port": fmt.Sprintf("%d", addr.Port),
		}).SetTimestamp(time.Now())
		_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
	}
}

// Stop closes the UDP socket (idempotent).
func (n *UDPInNode) Stop() error {
	var err error
	n.stopOnce.Do(func() {
		if n.conn != nil {
			err = n.conn.Close()
		}
	})
	return err
}

// Validate checks the udpIn node configuration
func (n *UDPInNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Port <= 0 || n.config.Port > 65535 {
		return fmt.Errorf("udpIn requires a valid port (1-65535)")
	}
	return nil
}

// NewUDPInNode creates a new udpIn node
func NewUDPInNode(node nodestore.NodeInterface, config *UDPInConfig, host nodebase.NodeHost) *UDPInNode {
	return &UDPInNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute binds the UDP socket and starts the datagram read loop.
