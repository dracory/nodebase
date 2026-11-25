// Package mqttout contains the mqttOut node implementation.
package mqttout

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"strings"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(MQTTOutConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewMQTTOutNode(n, c, e), nil
}

// init registers the mqttOut node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "mqttOut",
			Name:        "MQTT Out",
			Description: "Publishes the message to an MQTT broker topic",
			Icon:        "podcast",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the mqttOut node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"broker": map[string]string{
			"type":        "string",
			"title":       "Broker",
			"description": "Broker address as host:port or tcp://host:port, or an mqttBroker config node id (Node-RED shared broker)",
		},
		"clientid": map[string]string{
			"type":        "string",
			"title":       "Client ID",
			"description": "MQTT client identifier; empty lets the library generate one",
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
		"tlsinsecure": map[string]interface{}{
			"type":        "boolean",
			"title":       "Skip Peer Verification",
			"description": "Disables broker certificate verification (insecure opt-out)",
			"default":     false,
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
		"topic": map[string]string{
			"type":        "string",
			"title":       "Topic",
			"description": "Publish topic; the message's topic metadata overrides it",
		},
		"qos": map[string]interface{}{
			"type":    "integer",
			"title":   "QoS",
			"enum":    []int{0, 1, 2},
			"default": 0,
		},
		"retain": map[string]interface{}{
			"type":    "boolean",
			"title":   "Retain",
			"default": false,
		},
		"username": map[string]interface{}{
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
	},
	"required": []string{"broker"},
}

// MQTTOutConfig holds configuration for the mqttOut node. The embedded
// MQTTConnConfig carries the connection block (broker address or an
// mqttBroker config-node id, credentials, clientID, clean session,
// keepalive, TLS and last-will fields).
type MQTTOutConfig struct {
	nodebase.MQTTConnConfig
	// Topic is the publish topic (the message's topic metadata overrides it).
	Topic string `json:"topic"`
	// QoS is the publish quality of service (0|1|2).
	QoS int `json:"qos"`
	// Retain sets the MQTT retain flag on published messages.
	Retain bool `json:"retain"`
}

// MQTTOutNode publishes the message Data to a broker topic and passes
// the input through unchanged. Connections are shared per workflow via
// the host-state MQTTClientPool.
type MQTTOutNode struct {
	*nodebase.BaseNode
	config     *MQTTOutConfig
	host       nodebase.NodeHost
	workflowID string
}

// Execute serializes Data, publishes it and returns the input payload.
func (n *MQTTOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	topic := n.config.Topic
	if input != nil {
		if t, ok := input.GetMetadata()["topic"].(string); ok && t != "" {
			topic = t
		}
	}
	if topic == "" {
		return nil, errors.New("mqttOut requires a topic (config or message metadata)")
	}

	payload, err := nodebase.MQTTPayloadBytes(input)
	if err != nil {
		return nil, err
	}

	pool := n.host.GetOrCreateNodeState(n.workflowID, nodebase.MQTTClientsKey, func() interface{} {
		return nodebase.NewMQTTClientPool()
	}).(*nodebase.MQTTClientPool)

	opts, err := nodebase.ResolveMQTTOpts(n.host, n.workflowID, &n.config.MQTTConnConfig)
	if err != nil {
		return nil, err
	}
	client, err := pool.Client(opts, "mqtt-out-"+n.GetID())
	if err != nil {
		return nil, err
	}

	token := client.Publish(topic, byte(nodebase.ClampQoS(n.config.QoS)), n.config.Retain, payload)
	if !token.WaitTimeout(10 * time.Second) {
		return nil, fmt.Errorf("mqttOut publish to %q timed out", topic)
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqttOut publish to %q failed: %w", topic, err)
	}
	return input, nil
}

// Validate checks the mqttOut node configuration
func (n *MQTTOutNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(n.config.Broker) == "" {
		return errors.New("mqttOut requires a broker (host:port or mqttBroker node id)")
	}
	if (n.config.TLSCert == "") != (n.config.TLSKey == "") {
		return errors.New("mqttOut: tlscert and tlskey must be configured together")
	}
	return nil
}

// NewMQTTOutNode creates a new mqttOut node
func NewMQTTOutNode(node nodestore.NodeInterface, config *MQTTOutConfig, host nodebase.NodeHost) *MQTTOutNode {
	return &MQTTOutNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute serializes Data, publishes it and returns the input payload.
