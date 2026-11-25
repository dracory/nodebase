// Package udpin contains the udpIn node implementation.
package udpin

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"net"
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
		"multicast": map[string]interface{}{
			"type":    "string",
			"title":   "Listen For",
			"enum":    []string{"false", "true"},
			"default": "false",
			"description": "false: plain UDP datagrams; " +
				"true: multicast datagrams for Group",
		},
		"group": map[string]interface{}{
			"type":        "string",
			"title":       "Group",
			"description": "Multicast group to join (e.g. 225.0.18.83)",
			"showIf":      map[string]interface{}{"multicast": "true"},
		},
		"iface": map[string]interface{}{
			"type":        "string",
			"title":       "Interface",
			"description": "Optional local interface name for the multicast join",
			"showIf":      map[string]interface{}{"multicast": "true"},
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"title":       "Port",
			"description": "UDP port to listen on",
		},
		"ipv": map[string]interface{}{
			"type":    "string",
			"title":   "Using",
			"enum":    []string{"udp4", "udp6"},
			"default": "udp4",
		},
		"datatype": map[string]interface{}{
			"type":    "string",
			"title":   "Output",
			"enum":    []string{"utf8", "buffer", "base64"},
			"default": "utf8",
			"description": "utf8: string payload; buffer: raw bytes; " +
				"base64: base64-encoded string",
		},
	},
	"required": []string{"port"},
}

// UDPInConfig holds configuration for the udpIn node.
type UDPInConfig struct {
	// Port is the UDP port to listen on.
	Port int `json:"port"`
	// Multicast joins Group when "true" (a Node-RED enum-as-string
	// field — imports serialize it as "true"/"false").
	Multicast string `json:"multicast"`
	// Group is the multicast group address to join.
	Group string `json:"group"`
	// Iface names the local interface for the multicast join.
	Iface string `json:"iface"`
	// IPv selects the socket family: "udp4" (default) | "udp6".
	IPv string `json:"ipv"`
	// Datatype selects the payload type: "utf8" (string — default,
	// matching the previous always-string behavior), "buffer"
	// (raw []byte) or "base64" (base64-encoded string).
	Datatype string `json:"datatype"`
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

// Execute binds the UDP socket — or joins the multicast group when
// configured — and starts the datagram read loop.
func (n *UDPInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	network := n.config.IPv
	if network == "" {
		network = "udp4"
	}
	var conn *net.UDPConn
	var err error
	if n.config.Multicast == "true" {
		group := net.ParseIP(n.config.Group)
		if group == nil {
			return nil, fmt.Errorf("udpIn invalid multicast group %q", n.config.Group)
		}
		var iface *net.Interface
		if n.config.Iface != "" {
			iface, err = net.InterfaceByName(n.config.Iface)
			if err != nil {
				return nil, fmt.Errorf("udpIn interface %q: %w", n.config.Iface, err)
			}
		}
		conn, err = net.ListenMulticastUDP(network, iface, &net.UDPAddr{IP: group, Port: n.config.Port})
		if err != nil {
			return nil, fmt.Errorf("udpIn cannot join %s:%d: %w", n.config.Group, n.config.Port, err)
		}
	} else {
		conn, err = net.ListenUDP(network, &net.UDPAddr{Port: n.config.Port})
		if err != nil {
			return nil, fmt.Errorf("udpIn cannot listen on :%d: %w", n.config.Port, err)
		}
	}
	n.conn = conn
	go n.readLoop()
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening", "port": n.config.Port}).SetTimestamp(time.Now()), nil
}

// payload converts a datagram into the configured output type.
func (n *UDPInNode) payload(b []byte) interface{} {
	switch n.config.Datatype {
	case "buffer":
		return append([]byte(nil), b...)
	case "base64":
		return base64.StdEncoding.EncodeToString(b)
	default: // "utf8" and "" — the legacy string payload
		return string(b)
	}
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
		payload := nodestore.NewMessage().SetPayload(n.payload(buf[:nr])).SetMetadata(map[string]interface{}{
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
	switch n.config.Multicast {
	case "", "false":
	case "true":
		if net.ParseIP(n.config.Group) == nil {
			return fmt.Errorf("udpIn multicast requires a valid group address")
		}
	default:
		return fmt.Errorf("invalid udpIn multicast %q (false|true)", n.config.Multicast)
	}
	switch n.config.IPv {
	case "", "udp4", "udp6":
	default:
		return fmt.Errorf("invalid udpIn ipv %q (udp4|udp6)", n.config.IPv)
	}
	switch n.config.Datatype {
	case "", "utf8", "buffer", "base64":
	default:
		return fmt.Errorf("invalid udpIn datatype %q (utf8|buffer|base64)", n.config.Datatype)
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
