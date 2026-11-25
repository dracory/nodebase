// Package change contains the change node implementation.
package change

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(ChangeConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	if err := translateNRRules(n, c); err != nil {
		return nil, err
	}
	return NewChangeNode(n, c), nil
}

// translateNRRules rewrites c.Rules when they arrived in Node-RED's
// serialization ({t:"set",p:"payload.x",pt:"msg",to:"1",tot:"num"})
// instead of the native {op,field,value} shape. Supported: set/delete/
// move with str/num/json/bool/msg values. Env vars, JSONata, regex
// replace and flow/global context have no native equivalent and fail
// loudly at deploy.
func translateNRRules(n nodestore.NodeInterface, c *ChangeConfig) error {
	raw, ok := n.GetData()["rules"].([]interface{})
	if !ok {
		return nil
	}
	native := true
	for _, r := range raw {
		if m, ok := r.(map[string]interface{}); ok {
			if _, hasOp := m["op"]; !hasOp {
				native = false
			}
		}
	}
	if native {
		return nil
	}
	translated := make([]ChangeRule, 0, len(raw))
	for i, r := range raw {
		m, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		t, _ := m["t"].(string)
		pt, _ := m["pt"].(string)
		p, _ := m["p"].(string)
		field := nrField(p, pt)
		if pt == "flow" || pt == "global" {
			return fmt.Errorf("change: rule %d uses %s context — not supported", i, pt)
		}
		switch t {
		case "set":
			tot, _ := m["tot"].(string)
			to, _ := m["to"].(string)
			switch tot {
			case "msg":
				translated = append(translated, ChangeRule{Op: "set", Field: field, FromField: to})
			case "", "str", "num", "json", "bool", "bin":
				translated = append(translated, ChangeRule{Op: "set", Field: field, Value: to, ValueType: tot})
			default:
				return fmt.Errorf("change: rule %d uses Node-RED value type %q — not supported", i, tot)
			}
		case "delete":
			translated = append(translated, ChangeRule{Op: "delete", Field: field})
		case "move":
			to, _ := m["to"].(string)
			tot, _ := m["tot"].(string)
			if tot != "" && tot != "msg" {
				return fmt.Errorf("change: rule %d move target uses type %q — not supported", i, tot)
			}
			translated = append(translated, ChangeRule{Op: "move", Field: field, Value: nrField(to, "msg")})
		case "change":
			return fmt.Errorf("change: rule %d uses regex change — not supported", i)
		default:
			if t != "" {
				return fmt.Errorf("change: rule %d has unknown Node-RED type %q", i, t)
			}
		}
	}
	c.Rules = translated
	return nil
}

// nrField maps a Node-RED rule target (p + pt) onto the native field
// convention: "payload"/"" is the whole payload, "payload.x" is a path
// inside it, other msg.* properties use the "msg." metadata prefix.
func nrField(p, pt string) string {
	if pt == "" {
		pt = "msg"
	}
	switch {
	case p == "payload":
		return "payload"
	case strings.HasPrefix(p, "payload."):
		return strings.TrimPrefix(p, "payload.")
	case pt == "flow" || pt == "global":
		return p // caller rejects pt flow/global anyway
	default:
		return "msg." + p
	}
}

// init registers the change node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "change",
			Name:        "Change",
			Description: "Sets, moves or deletes fields on the payload",
			Icon:        "edit",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the change node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"rules": map[string]interface{}{
			"type":        "array",
			"title":       "Rules",
			"description": "Ordered list of rules, each {op: set|delete|move, field, value}. Field is a dotted path; \"payload\" targets the whole payload.",
			"items": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"op": map[string]interface{}{
						"type": "string",
						"enum": []string{"set", "delete", "move"},
					},
					"field": map[string]string{
						"type":  "string",
						"title": "Field",
					},
					"value": map[string]interface{}{
						"type":      "string",
						"title":     "Value",
						"format":    "typedinput",
						"typeField": "valueType",
					},
				},
			},
		},
	},
}

// ChangeRule is a single ordered mutation applied to the message.
// Field is a dotted path; "payload" (or empty) targets msg.Payload,
// a "msg." prefix (e.g. "msg.statusCode") targets msg-level properties
// stored in Metadata — Node-RED's msg.statusCode/msg.headers
// equivalent — and anything else is a path inside msg.Payload.
type ChangeRule struct {
	Op    string `json:"op"` // set | delete | move
	Field string `json:"field"`
	Value string `json:"value"`
	// ValueType is Node-RED's tot: how Value is interpreted for "set"
	// — "str" (default), "num", "bool", "json", or "msg" (read the
	// value from a message property path, same as FromField).
	ValueType string `json:"valueType,omitempty"`
	// FromField, when set, takes the value from a message path
	// (msg.topic, payload.user, …) instead of the literal Value —
	// Node-RED's set-with-tot:"msg".
	FromField string `json:"fromField,omitempty"`
}

// ChangeConfig holds configuration for the change node
type ChangeConfig struct {
	Rules []ChangeRule `json:"rules"`
}

// ChangeNode mutates msg.Payload via an ordered list of rules, the
// equivalent of Node-RED's change node. A failing rule is skipped so
// one bad rule does not abort the flow.
type ChangeNode struct {
	*nodebase.BaseNode
	config *ChangeConfig
}

// NewChangeNode creates a new change node
func NewChangeNode(node nodestore.NodeInterface, config *ChangeConfig) *ChangeNode {
	return &ChangeNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute applies each configured rule in order and returns the
// mutated payload
func (n *ChangeNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	for _, rule := range n.config.Rules {
		switch rule.Op {
		case "set":
			n.applySet(input, rule)
		case "delete":
			if rule.Field == "" || rule.Field == "payload" {
				input.SetPayload(nil) // Node-RED: delete msg.payload
				continue
			}
			if path, ok := nodebase.MsgPath(rule.Field); ok {
				nodebase.DeletePath(input.GetMetadata(), path)
			} else {
				nodebase.DeletePath(input.GetPayload(), rule.Field)
			}
		case "move":
			n.applyMove(input, rule)
		}
	}
	return input, nil
}

// applySet handles the "set" op: the special field "payload"/"" replaces
// msg.Payload, "msg.x" writes into Metadata, otherwise the dotted path
// is set inside the data map.
// The value is stored as parsed JSON when it parses, else as a string.
func (n *ChangeNode) applySet(input nodestore.MessageInterface, rule ChangeRule) {
	var value interface{}
	src := rule.FromField
	if src == "" && rule.ValueType == "msg" {
		src = rule.Value // typedInput "msg": Value is the source path
	}
	if src != "" {
		// Copy semantics (Node-RED tot:"msg"): read another message
		// property — metadata first, then inside the payload map.
		if path, ok := nodebase.MsgPath(src); ok {
			value, _ = nodebase.GetPath(input.GetMetadata(), path)
		} else if src == "payload" {
			value = input.GetPayload()
		} else if m, ok := input.GetPayload().(map[string]interface{}); ok {
			value, _ = nodebase.GetPath(m, src)
		}
	} else {
		switch rule.ValueType {
		case "num":
			if f, ok := nodebase.ParseNumericString(rule.Value); ok {
				value = f
			} else {
				value = rule.Value
			}
		case "bool":
			value = rule.Value == "true" || rule.Value == "1"
		case "json":
			var v interface{}
			if err := json.Unmarshal([]byte(rule.Value), &v); err == nil {
				value = v
			} else {
				value = rule.Value
			}
		case "str":
			value = rule.Value // explicit string: no JSON coercion
		default: // "" legacy — parse-if-JSON for backward compatibility
			value = nodebase.ParseJSONOrString(rule.Value)
		}
	}

	if rule.Field == "" || rule.Field == "payload" {
		input.SetPayload(value)
		return
	}
	if path, ok := nodebase.MsgPath(rule.Field); ok {
		if input.GetMetadata() == nil {
			input.SetMetadata(map[string]interface{}{})
		}
		nodebase.SetPath(input.GetMetadata(), path, value)
		return
	}

	m, ok := input.GetPayload().(map[string]interface{})
	if !ok {
		m = map[string]interface{}{}
		input.SetPayload(m)
	}
	nodebase.SetPath(m, rule.Field, value)
}

// applyMove handles the "move" op: reads the source field, removes it,
// then writes the value at the destination path held in rule.Value
func (n *ChangeNode) applyMove(input nodestore.MessageInterface, rule ChangeRule) {
	src := rule.Field
	dst := rule.Value
	if src == "" || dst == "" || src == dst {
		return
	}

	var value interface{}
	if src == "payload" {
		if dst == "payload" {
			return // payload -> payload is a no-op
		}
		// Clear the source BEFORE writing the destination: the payload
		// may move into a field of itself, which is safe only once the
		// root no longer references it (otherwise a cycle forms).
		value = input.GetPayload()
		input.SetPayload(nil)
	} else if srcPath, ok := nodebase.MsgPath(src); ok {
		v, found := nodebase.GetPath(input.GetMetadata(), srcPath)
		if !found {
			return
		}
		value = v
		nodebase.DeletePath(input.GetMetadata(), srcPath)
	} else {
		m, ok := input.GetPayload().(map[string]interface{})
		if !ok {
			return
		}
		v, found := nodebase.GetPath(m, src)
		if !found {
			return
		}
		value = v
		nodebase.DeletePath(input.GetPayload(), src)
	}

	if dst == "payload" {
		input.SetPayload(value)
		return
	}
	if dstPath, ok := nodebase.MsgPath(dst); ok {
		if input.GetMetadata() == nil {
			input.SetMetadata(map[string]interface{}{})
		}
		nodebase.SetPath(input.GetMetadata(), dstPath, value)
		return
	}
	m2, ok := input.GetPayload().(map[string]interface{})
	if !ok {
		m2 = map[string]interface{}{}
		input.SetPayload(m2)
	}
	nodebase.SetPath(m2, dst, value)
}
