// Package mqttout contains the mqttOut node implementation.
package mqttout

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"github.com/spf13/cast"
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
			"description": "Publish topic; when blank the message's topic metadata is used instead",
		},
		"qos": map[string]interface{}{
			"type":        "integer",
			"title":       "QoS",
			"enum":        []int{0, 1, 2},
			"default":     0,
			"description": "Publish QoS; when blank the message's qos metadata is used instead",
		},
		"retain": map[string]interface{}{
			"type":        "boolean",
			"title":       "Retain",
			"default":     false,
			"description": "Retain flag; when blank the message's retain metadata is used instead",
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
	// Topic is the publish topic; when blank the message's topic metadata is used (Node-RED precedence).
	Topic string `json:"topic"`
	// QoS is the publish quality of service (0|1|2). A nil pointer
	// marks a blank config — Node-RED serializes blank qos as "" and
	// msg.qos is the fallback — so it stays distinct from explicit 0.
	QoS *int `json:"qos"`
	// Retain sets the MQTT retain flag on published messages. Nil
	// means the config was blank ("") and msg.retain is the fallback.
	Retain *bool `json:"retain"`
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

// publishTopic resolves the publish topic with Node-RED precedence:
// the configured topic wins; the message's topic metadata is only the
// fallback when the config is blank.
func publishTopic(configTopic string, input nodestore.MessageInterface) string {
	if t := strings.TrimSpace(configTopic); t != "" {
		return t
	}
	if input != nil {
		if t, ok := input.GetMetadata()["topic"].(string); ok {
			return t
		}
	}
	return ""
}

// publishQoS resolves the publish QoS with the same Node-RED
// precedence as publishTopic: a configured qos wins; msg.qos metadata
// is only the fallback when the config is blank (nil — Node-RED
// serializes blank qos as ""). Defaults to 0 when neither is set.
func publishQoS(configQoS *int, input nodestore.MessageInterface) int {
	if configQoS != nil {
		return nodebase.ClampQoS(*configQoS)
	}
	if input != nil {
		if v, ok := input.GetMetadata()["qos"]; ok {
			if f, ok := nodebase.ParseNumeric(v); ok {
				return nodebase.ClampQoS(int(f))
			}
		}
	}
	return 0
}

// publishRetain resolves the retain flag with Node-RED precedence:
// an explicitly configured retain (including "false") wins; msg.retain
// metadata is only the fallback when the config is blank. Defaults to
// false when neither is set.
func publishRetain(configRetain *bool, input nodestore.MessageInterface) bool {
	if configRetain != nil {
		return *configRetain
	}
	if input != nil {
		if v, ok := input.GetMetadata()["retain"]; ok {
			if b, err := cast.ToBoolE(v); err == nil {
				return b
			}
		}
	}
	return false
}

// Execute serializes Data, publishes it and returns the input payload.
func (n *MQTTOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	topic := publishTopic(n.config.Topic, input)
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

	token := client.Publish(topic, byte(publishQoS(n.config.QoS, input)), publishRetain(n.config.Retain, input), payload)
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
