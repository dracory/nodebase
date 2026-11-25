// Package mqttin contains the mqttIn node implementation.
package mqttin

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"strings"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(MQTTInConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewMQTTInNode(n, c, e), nil
}

// init registers the mqttIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "mqttIn",
			Name:        "MQTT In",
			Description: "Subscribes to an MQTT broker topic and emits received messages",
			Icon:        "podcast",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the mqttIn node.
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
			"description": "Subscription topic; MQTT wildcards + and # are supported. A raw $share/<group>/<topic> filter also works",
		},
		"shareGroup": map[string]string{
			"type":        "string",
			"title":       "Share Group",
			"description": "Optional MQTT shared-subscription group; the filter becomes $share/<group>/<topic> and the broker load-balances messages across subscribers in the group",
		},
		"qos": map[string]interface{}{
			"type":    "integer",
			"title":   "QoS",
			"enum":    []int{0, 1, 2},
			"default": 0,
		},
		"datatype": map[string]interface{}{
			"type":    "string",
			"title":   "Payload Type",
			"enum":    []string{"auto", "utf8", "json", "base64"},
			"default": "auto",
			"description": "auto: try JSON parse, fall back to string; " +
				"utf8: plain string; json: parse (failures emit raw string); " +
				"base64: encode raw bytes",
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
	"required": []string{"broker", "topic"},
}

// MQTTInConfig holds configuration for the mqttIn node. Broker settings
// are inline per node — the Node-RED shared broker config-node concept
// is deferred (see docs/ai/tasks 30-mqtt-nodes).
type MQTTInConfig struct {
	// Broker is host:port or a full URL (tcp://host:port).
	Broker string `json:"broker"`
	// Topic is the subscription topic; MQTT wildcards + and # work.
	Topic string `json:"topic"`
	// QoS is the subscription quality of service (0|1|2).
	QoS int `json:"qos"`
	// Datatype selects payload decoding: "auto" (JSON parse, else
	// string), "utf8" (string), "json" (parse or error), "base64".
	Datatype string `json:"datatype"`
	// ShareGroup, when set, turns the subscription into an MQTT shared
	// subscription: the filter becomes $share/<group>/<topic> and the
	// broker load-balances deliveries across all subscribers in the
	// group (active-active triggers for multi-runner deployments).
	// A raw $share/<group>/<topic> value in Topic also works and is
	// passed through unchanged.
	ShareGroup string `json:"shareGroup"`
	// Username/Password are optional broker credentials. They are
	// credential-typed fields: stored outside the exported flow
	// document and merged back in at deploy time.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// subscriptionTopic returns the effective subscription filter: the raw
// topic, or $share/<group>/<topic> when a share group is configured.
func (c *MQTTInConfig) subscriptionTopic() string {
	topic := strings.TrimSpace(c.Topic)
	group := strings.TrimSpace(c.ShareGroup)
	if group == "" || strings.HasPrefix(topic, "$share/") {
		return topic
	}
	return "$share/" + group + "/" + topic
}

// MQTTInNode subscribes to an MQTT broker topic on deploy and emits
// every received message downstream. It is a Stopper: the client lives
// on the node instance itself (activation runs Execute once and the
// same instance is kept for Stop).
type MQTTInNode struct {
	*nodebase.BaseNode
	config     *MQTTInConfig
	host       nodebase.NodeHost
	workflowID string
	client     mqtt.Client
	stopOnce   sync.Once
}

// Execute connects to the broker and subscribes. The connect happens
// synchronously so an unreachable broker surfaces as a deploy error.
func (n *MQTTInNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	client, err := nodebase.MQTTConnect(n.config.Broker, "mqtt-in-"+n.GetID(), n.config.Username, n.config.Password)
	if err != nil {
		return nil, err
	}
	n.client = client

	filter := n.config.subscriptionTopic()
	token := client.Subscribe(filter, byte(nodebase.ClampQoS(n.config.QoS)), n.onMessage)
	if !token.WaitTimeout(10 * time.Second) {
		client.Disconnect(250)
		return nil, fmt.Errorf("mqttIn subscribe to %q timed out", filter)
	}
	if err := token.Error(); err != nil {
		client.Disconnect(250)
		return nil, fmt.Errorf("mqttIn subscribe to %q failed: %w", filter, err)
	}

	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "subscribed", "broker": n.config.Broker, "topic": filter}).SetTimestamp(time.Now()), nil
}

// onMessage converts a broker message into a nodestore.MessageInterface and emits
// it downstream (fire-and-forget).
func (n *MQTTInNode) onMessage(_ mqtt.Client, msg mqtt.Message) {
	data, err := n.decode(msg.Payload())
	if err != nil {
		data = string(msg.Payload())
	}
	_ = n.host.Emit(n.workflowID, n.GetID(), nodestore.NewMessage().SetPayload(data).SetMetadata(map[string]interface{}{
		"topic": msg.Topic(),
		"qos":   int(msg.Qos()),
	}).SetTimestamp(time.Now()), nil)
}

// decode interprets the payload bytes per the configured datatype.
func (n *MQTTInNode) decode(raw []byte) (interface{}, error) {
	switch n.config.Datatype {
	case "utf8":
		return string(raw), nil
	case "json":
		var v interface{}
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "base64":
		return base64.StdEncoding.EncodeToString(raw), nil
	default: // "auto": JSON parse, fall back to string
		var v interface{}
		if err := json.Unmarshal(raw, &v); err == nil {
			return v, nil
		}
		return string(raw), nil
	}
}

// Stop disconnects the broker client.
func (n *MQTTInNode) Stop() error {
	n.stopOnce.Do(func() {
		if n.client != nil && n.client.IsConnected() {
			n.client.Disconnect(500)
		}
	})
	return nil
}

// Validate checks the mqttIn node configuration
func (n *MQTTInNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(n.config.Broker) == "" {
		return errors.New("mqttIn requires a broker (host:port)")
	}
	if strings.TrimSpace(n.config.Topic) == "" {
		return errors.New("mqttIn requires a topic")
	}
	if group := strings.TrimSpace(n.config.ShareGroup); group != "" {
		if strings.HasPrefix(strings.TrimSpace(n.config.Topic), "$share/") {
			return errors.New("mqttIn shareGroup is redundant when the topic already starts with $share/")
		}
		if strings.ContainsAny(group, "/+#") {
			return fmt.Errorf("invalid mqttIn shareGroup %q (must not contain /, + or #)", group)
		}
	}
	switch n.config.Datatype {
	case "", "auto", "utf8", "json", "base64":
		return nil
	default:
		return fmt.Errorf("invalid mqttIn datatype %q (auto|utf8|json|base64)", n.config.Datatype)
	}
}

// NewMQTTInNode creates a new mqttIn node
func NewMQTTInNode(node nodestore.NodeInterface, config *MQTTInConfig, host nodebase.NodeHost) *MQTTInNode {
	return &MQTTInNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute connects to the broker and subscribes. The connect happens
// synchronously so an unreachable broker surfaces as a deploy error.
