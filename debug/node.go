// Package debug contains the debug node implementation.
package debug

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"strings"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(DebugConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewDebugNode(n, c), nil
}

// init registers the debug node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "debug",
			Name:        "Debug",
			Description: "Displays the message payload in the debug panel",
			Icon:        "stethoscope",
			Category:    "Debug",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the debug node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"active": map[string]interface{}{
			"type":        "boolean",
			"title":       "Active",
			"description": "false mutes the node — deployed but emits nothing",
			"default":     true,
		},
		"console": map[string]interface{}{
			"type":        "boolean",
			"title":       "Also Print to Console",
			"description": "Print the formatted value to stdout as well",
			"default":     false,
		},
		"complete": map[string]interface{}{
			"type":    "string",
			"title":   "Output",
			"default": "payload",
			"description": "payload (or empty): the message data · true: complete msg object " +
				"incl. metadata · false: bare marker · any other string is a msg.<path> " +
				"(e.g. \"topic\" or \"payload.user\"). Node-RED's JSONata \"expression\" " +
				"option is not supported — use a function node.",
		},
		"statusVal": map[string]interface{}{
			"type":        "string",
			"title":       "Status Value",
			"description": "Optional property path (\"payload\" or a dotted path) also shown as node status text",
		},
	},
}

// DebugConfig holds configuration for the debug node
type DebugConfig struct {
	// Active toggles the node; false means deployed but muted (no events)
	Active *bool `json:"active"`
	// Console also prints the formatted value to stdout
	Console bool `json:"console"`
	// Complete selects what is displayed: "payload" (Data only),
	// "true" (the whole payload including Metadata) or "false"
	// (a bare marker, Node-RED's "node name only" behaviour)
	Complete string `json:"complete"`
	// StatusVal is an optional property path whose value is also
	// reported as the node's status text (emitted as an extra event)
	StatusVal string `json:"statusVal"`
}

// DebugNode passes the payload through unchanged; the formatted debug
// value is emitted into the workflow event log by the host so it
// shows up in the editor's debug panel (Node-RED debug node)
type DebugNode struct {
	*nodebase.BaseNode
	config *DebugConfig
}

// NewDebugNode creates a new debug node
func NewDebugNode(node nodestore.NodeInterface, config *DebugConfig) *DebugNode {
	return &DebugNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute passes the input payload through unchanged
func (n *DebugNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// PostRunEvents implements RunEventer: the debug node emits
// the formatted message value into the event log so it renders in the
// editor's debug panel; console=true also prints it to stdout, and a
// configured statusVal is reported as an extra "status" event.
// active=false mutes the node entirely.
func (n *DebugNode) PostRunEvents(node nodestore.NodeInterface, out nodestore.MessageInterface) []nodebase.RunEvent {
	message, ok := debugMessage(node, out)
	if !ok {
		return nil
	}
	events := []nodebase.RunEvent{{Level: "debug", Message: message}}
	if console, _ := node.GetData()["console"].(bool); console {
		fmt.Println(message)
	}
	if v, ok := debugStatusValue(node, out); ok {
		raw, err := json.Marshal(v)
		if err != nil {
			raw = []byte(fmt.Sprintf("%v", v))
		}
		path, _ := node.GetData()["statusVal"].(string)
		events = append(events, nodebase.RunEvent{
			Level:   "debug",
			Message: fmt.Sprintf("status %s: %s", path, raw),
		})
	}
	return events
}

// debugActive reports whether the debug node is enabled (default true)
func debugActive(node nodestore.NodeInterface) bool {
	if active, ok := node.GetData()["active"].(bool); ok {
		return active
	}
	return true
}

// debugMessage formats the value a debug node displays for the given
// payload, honouring the "complete" config option. The boolean result
// is false when the node is muted (active=false) and nothing should
// be emitted.
func debugMessage(node nodestore.NodeInterface, out nodestore.MessageInterface) (string, bool) {
	if !debugActive(node) {
		return "", false
	}

	complete, _ := node.GetData()["complete"].(string)
	var value interface{}
	switch complete {
	case "true":
		value = out // the whole payload, Metadata included
	case "false":
		return "debug", true // bare marker — Node-RED "node name only"
	case "", "payload":
		if out != nil {
			value = out.GetPayload()
		}
	default:
		// Node-RED parity: any other string is a msg.<path> — a
		// "payload." prefix drills into Data (msg.payload.foo);
		// other paths resolve against Metadata first, then Data.
		if out != nil {
			if rest, ok := strings.CutPrefix(complete, "payload."); ok {
				if m, ok := out.GetPayload().(map[string]interface{}); ok {
					value, _ = nodebase.GetPath(m, rest)
				}
			} else if v, ok := nodebase.GetPath(out.GetMetadata(), complete); ok {
				value = v
			} else if m, ok := out.GetPayload().(map[string]interface{}); ok {
				value, _ = nodebase.GetPath(m, complete)
			}
		}
	}

	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", value), true
	}
	return string(raw), true
}

// debugStatusValue resolves the node's statusVal property path against
// the payload. Paths are looked up in Metadata first, then inside Data
// (dotted paths supported), mirroring Node-RED's statusVal which can
// point at any message property.
func debugStatusValue(node nodestore.NodeInterface, out nodestore.MessageInterface) (interface{}, bool) {
	path, _ := node.GetData()["statusVal"].(string)
	if path == "" || out == nil {
		return nil, false
	}
	// "payload" is the whole Data, matching the convention used by
	// every other node's property option
	if path == "payload" {
		return out.GetPayload(), true
	}
	if v, ok := nodebase.GetPath(out.GetMetadata(), path); ok {
		return v, true
	}
	if m, ok := out.GetPayload().(map[string]interface{}); ok {
		return nodebase.GetPath(m, path)
	}
	return nil, false
}
