// Package inject contains the inject node implementation.
package inject

import (
	"context"
	_ "embed"
	"encoding/json"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(InjectConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewInjectNode(n, c, e), nil
}

// init registers the inject node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "inject",
			Name:        "Inject",
			Description: "Manually or periodically injects a message",
			Icon:        "play-circle",
			Category:    "Triggers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the inject node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"payloadType": map[string]interface{}{
			"type":        "string",
			"title":       "Payload Type",
			"enum":        []string{"timestamp", "date", "json", "string", "num", "bool", "env"},
			"description": "timestamp/date: current time in ms · json: parse the payload field · string: literal text · num: parsed number · bool: true/false · env: payload names an environment variable",
			"default":     "timestamp",
		},
		"payload": map[string]interface{}{
			"type":        "string",
			"format":      "textarea",
			"title":       "Payload",
			"description": "Data injected as the message — interpreted per Payload Type (unused for timestamp/date)",
		},
		"topic": map[string]interface{}{
			"type":        "string",
			"title":       "Topic",
			"description": "Optional topic stamped into the message metadata (Node-RED msg.topic)",
		},
		"once": map[string]interface{}{
			"type":        "boolean",
			"title":       "Fire Once on Deploy",
			"description": "Inject the payload immediately when the workflow is deployed",
			"default":     false,
		},
		"interval": map[string]interface{}{
			"type":        "number",
			"title":       "Interval",
			"description": "Repeat period in Interval Units; 0 = no repeat",
			"default":     0,
		},
		"intervalUnits": map[string]interface{}{
			"type":        "string",
			"title":       "Interval Units",
			"enum":        []string{"s", "min", "hr", "day"},
			"default":     "s",
			"description": "Time unit for the repeat interval",
		},
	},
}

// InjectConfig holds configuration for the inject node
type InjectConfig struct {
	// Payload is the message data — interpreted per PayloadType
	Payload string `json:"payload"`
	// PayloadType selects how Payload is interpreted: "timestamp"
	// (current time in ms, Node-RED's default), "date" (same),
	// "json" (parsed), "string"/"str" (literal text), "num" (parsed
	// number), "bool" ("true"/"false"), or "env" (Payload names the
	// environment variable to read). Empty defaults to timestamp when
	// no payload is configured, json otherwise.
	PayloadType string `json:"payloadType"`
	// Topic is stamped into Metadata["topic"] on every injected message
	// (Node-RED msg.topic)
	Topic string `json:"topic"`
	// Once fires the payload immediately on deploy
	Once bool `json:"once"`
	// Interval is the repeat period (0 = no repeat), in IntervalUnits
	Interval float64 `json:"interval"`
	// IntervalUnits scales Interval: "s" (default) | "min" | "hr" | "day"
	IntervalUnits string `json:"intervalUnits"`
	// Props is Node-RED's modern property list — each entry sets one
	// message property: "payload" and "topic" are the well-known ones;
	// anything else lands in Metadata. When a "payload" prop exists it
	// takes precedence over the legacy Payload/PayloadType fields.
	Props []InjectProp `json:"props"`
}

// InjectProp is one entry of Node-RED's inject props array:
// {"p":"payload","v":"Hello","vt":"str"}.
type InjectProp struct {
	P  string `json:"p"`  // property name ("payload", "topic", or custom)
	V  string `json:"v"`  // literal value (per VT)
	VT string `json:"vt"` // value type: str|num|bool|json|env|date|bin|msg|flow|global|jsonata
}

// InjectNode is a trigger that fires a configured payload once on
// deploy and/or repeatedly on an interval. Injects are fire-and-forget:
// no response is expected, so each message's pending entry is discarded
// right after injection (InjectMessage registers a pending channel
// keyed by msgID that nothing would otherwise resolve).
type InjectNode struct {
	*nodebase.BaseNode
	config          *InjectConfig
	host            nodebase.NodeHost
	workflowID      string
	stopCh          chan struct{}
	stopOnce        sync.Once
	payload         nodestore.MessageInterface // pre-parsed payload template
	payloadType     string
	payloadTemplate string
	propMetadata    map[string]interface{} // extra props entries → metadata
	topic           string                 // resolved topic (config or props)
}

// buildPayload materializes the message Data per the configured
// payload type. Called per fire so "timestamp" reflects the fire time,
// not the deploy time (Node-RED behavior).
func (n *InjectNode) buildPayload() nodestore.MessageInterface {
	var data interface{}
	switch n.payloadType {
	case "timestamp", "date":
		data = float64(time.Now().UnixMilli())
	case "string", "str":
		data = n.payloadTemplate
	case "num":
		if f, err := strconv.ParseFloat(strings.TrimSpace(n.payloadTemplate), 64); err == nil {
			data = f
		} else {
			data = float64(0)
		}
	case "bool":
		data = strings.TrimSpace(n.payloadTemplate) == "true"
	case "env":
		data = os.Getenv(n.payloadTemplate)
	default: // "json"
		if err := json.Unmarshal([]byte(n.payloadTemplate), &data); err != nil || data == nil {
			data = "hello"
		}
	}
	metadata := map[string]interface{}{"origin": "inject"}
	if n.topic != "" {
		metadata["topic"] = n.topic
	}
	for k, v := range n.propMetadata {
		metadata[k] = v
	}
	return nodestore.NewMessage().SetPayload(data).SetMetadata(metadata).SetTimestamp(time.Now())
}

// NewInjectNode creates a new inject node
func NewInjectNode(node nodestore.NodeInterface, config *InjectConfig, host nodebase.NodeHost) *InjectNode {
	return &InjectNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
		stopCh:     make(chan struct{}),
	}
}

// Execute parses the configured payload once, then starts the firing
// goroutines (once-shot and/or interval ticker).
func (n *InjectNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.payloadTemplate = n.config.Payload
	n.payloadType = n.config.PayloadType
	n.propMetadata = nil
	n.topic = n.config.Topic
	// Node-RED props array takes precedence over the legacy
	// payload/payloadType/topic fields when present.
	for _, p := range n.config.Props {
		switch p.P {
		case "payload":
			n.payloadTemplate = p.V
			n.payloadType = p.VT
		case "topic":
			n.topic = p.V
		default:
			// custom message property → metadata
			if n.propMetadata == nil {
				n.propMetadata = map[string]interface{}{}
			}
			n.propMetadata[p.P] = propValue(p.VT, p.V)
		}
	}
	if n.payloadType == "" {
		if n.payloadTemplate == "" {
			n.payloadType = "timestamp"
		} else {
			n.payloadType = "json"
		}
	}
	n.payload = n.buildPayload()

	if n.config.Once {
		go n.fire()
	}

	if n.config.Interval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(n.config.Interval * float64(n.intervalUnit())))
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					n.fire()
				case <-n.stopCh:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "armed"}).SetTimestamp(time.Now()), nil
}

// Fire injects a clone of the payload into the workflow — the
// host's FireInjectNode contract (a manual click on the inject
// button). Errors are ignored (e.g. the workflow may be undeploying).
// The pending response channel is discarded immediately — inject
// messages never get a reply.
func (n *InjectNode) Fire() {
	n.fire()
}

// fire injects a clone of the payload into the workflow.
func (n *InjectNode) fire() {
	// Fresh Metadata map per fire: InjectMessage mutates it (msgId)
	metadata := make(map[string]interface{}, len(n.payload.GetMetadata()))
	for k, v := range n.payload.GetMetadata() {
		metadata[k] = v
	}
	built := n.buildPayload()
	payload := nodestore.NewMessage().SetPayload(built.GetPayload()).SetMetadata(metadata).SetTimestamp(time.Now())
	_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
}

// propValue coerces an InjectProp value by its vt code. Unknown or
// unsupported types (jsonata, cred) pass through as the raw string.
func propValue(vt, v string) interface{} {
	switch vt {
	case "num":
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
		return float64(0)
	case "bool":
		return strings.TrimSpace(v) == "true"
	case "json":
		var data interface{}
		if err := json.Unmarshal([]byte(v), &data); err == nil {
			return data
		}
		return nil
	case "env":
		return os.Getenv(v)
	case "date":
		return float64(time.Now().UnixMilli())
	default: // str, msg, flow, global, bin, jsonata, cred
		return v
	}
}

// intervalUnit returns the repeat-period multiplier for IntervalUnits
// (default one second).
func (n *InjectNode) intervalUnit() time.Duration {
	switch n.config.IntervalUnits {
	case "min":
		return time.Minute
	case "hr":
		return time.Hour
	case "day":
		return 24 * time.Hour
	}
	return time.Second
}

// Stop halts the interval ticker (idempotent)
func (n *InjectNode) Stop() error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	return nil
}
