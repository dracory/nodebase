// Package mqttbroker contains the mqttBroker config node — the
// shared MQTT broker description mqttIn/mqttOut reference via their
// "broker" field (Node-RED mqtt-broker).
package mqttbroker

import (
	"context"
	_ "embed"
	"errors"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(nodebase.MQTTBrokerConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewMQTTBrokerNode(n, c), nil
}

// init registers the mqttBroker node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        nodebase.MQTTBrokerType,
			Name:        "MQTT Broker",
			Description: "Shared MQTT broker connection referenced by mqttIn/mqttOut nodes",
			Icon:        "podcast",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the mqttBroker node.
// Field spellings follow the Node-RED mqtt-broker export format.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"broker": map[string]string{
			"type":        "string",
			"title":       "Broker",
			"description": "Broker address as host:port or tcp://host:port (ssl:// for TLS)",
		},
		"port": map[string]string{
			"type":        "string",
			"title":       "Port",
			"description": "Optional separate broker port (Node-RED shape); merged onto a portless host",
		},
		"clientid": map[string]string{
			"type":        "string",
			"title":       "Client ID",
			"description": "MQTT client identifier; empty lets the library generate one",
		},
		"user": map[string]interface{}{
			"type":        "string",
			"format":      "credential",
			"title":       "Username",
			"description": "Optional broker username (credential: masked, never exported)",
		},
		"password": map[string]interface{}{
			"type":        "string",
			"format":      "credential",
			"title":       "Password",
			"description": "Optional broker password (credential: masked, never exported)",
		},
		"cleansession": map[string]interface{}{
			"type":    "boolean",
			"title":   "Clean Session",
			"default": true,
		},
		"keepalive": map[string]interface{}{
			"type":        "integer",
			"title":       "Keep Alive",
			"description": "MQTT keepalive in seconds; 0 uses the library default",
			"default":     0,
		},
		"usetls": map[string]interface{}{
			"type":        "boolean",
			"title":       "Use TLS",
			"description": "Dial the broker over TLS (implied by an ssl:// address)",
			"default":     false,
		},
		"verifyservercert": map[string]interface{}{
			"type":        "boolean",
			"title":       "Verify Peer Certificate",
			"description": "false disables peer verification (insecure; Node-RED verifyservercert)",
			"default":     true,
		},
		"tls": map[string]string{
			"type":        "string",
			"title":       "TLS Config",
			"description": "Optional tlsConfig node id for shared certificate material",
		},
		"tlsca": map[string]string{
			"type":        "string",
			"title":       "CA Bundle",
			"description": "Optional PEM CA bundle used to verify the broker",
		},
		"tlscert": map[string]string{
			"type":        "string",
			"title":       "Certificate",
			"description": "PEM client certificate; tlscert and tlskey must be set together",
		},
		"tlskey": map[string]string{
			"type":        "string",
			"title":       "Private Key",
			"description": "PEM private key paired with the certificate",
		},
		"tlsservername": map[string]string{
			"type":        "string",
			"title":       "Server Name",
			"description": "SNI / verification hostname override (default: the dialed host)",
		},
		"willtopic": map[string]string{
			"type":        "string",
			"title":       "Will Topic",
			"description": "Last-will topic the broker publishes when this client drops ungracefully",
		},
		"willpayload": map[string]string{
			"type":        "string",
			"title":       "Will Payload",
			"description": "Last-will message",
		},
		"willqos": map[string]interface{}{
			"type":    "integer",
			"title":   "Will QoS",
			"enum":    []int{0, 1, 2},
			"default": 0,
		},
		"willretain": map[string]interface{}{
			"type":    "boolean",
			"title":   "Will Retain",
			"default": false,
		},
	},
}

// MQTTBrokerNode is a config entity; its Execute is never invoked (no
// ports), existing only to satisfy the NodeExecutor contract.
type MQTTBrokerNode struct {
	*nodebase.BaseNode
	config *nodebase.MQTTBrokerConfig
}

// NewMQTTBrokerNode creates a new mqttBroker node
func NewMQTTBrokerNode(node nodestore.NodeInterface, config *nodebase.MQTTBrokerConfig) *MQTTBrokerNode {
	return &MQTTBrokerNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute passes the payload through — unreachable in practice since
// an mqttBroker node has no inputs.
func (n *MQTTBrokerNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// Validate checks the structural config: a broker address is required
// and inline cert/key must come as a pair. File readability is checked
// when the material is loaded at connect time.
func (n *MQTTBrokerNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Broker == "" {
		return errors.New("mqttBroker requires a broker (host:port)")
	}
	if (n.config.TLSCert == "") != (n.config.TLSKey == "") {
		return errors.New("mqttBroker: tlscert and tlskey must be configured together")
	}
	return nil
}
