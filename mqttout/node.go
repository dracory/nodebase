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
			"description": "Broker address as host:port or tcp://host:port",
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

// MQTTOutConfig holds configuration for the mqttOut node.
type MQTTOutConfig struct {
	// Broker is host:port or a full URL (tcp://host:port).
	Broker string `json:"broker"`
	// Topic is the publish topic (the message's topic metadata overrides it).
	Topic string `json:"topic"`
	// QoS is the publish quality of service (0|1|2).
	QoS int `json:"qos"`
	// Retain sets the MQTT retain flag on published messages.
	Retain bool `json:"retain"`
	// Username/Password are optional broker credentials. They are
	// credential-typed fields: stored outside the exported flow
	// document and merged back in at deploy time.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
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

	client, err := pool.Client(n.config.Broker, "mqtt-out-"+n.GetID(), n.config.Username, n.config.Password)
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
		return errors.New("mqttOut requires a broker (host:port)")
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
