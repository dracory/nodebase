// Package change contains the change node implementation.
package change

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
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
	// Fail loud at deploy when a rule needs context the host cannot
	// provide — a silently skipped write would diverge downstream.
	for _, r := range c.Rules {
		needsContext := r.FieldType == "flow" || r.FieldType == "global" ||
			r.ValueType == "flow" || r.ValueType == "global"
		if needsContext && (e == nil || e.ResolvedContextStore() == nil) {
			return nil, fmt.Errorf("change: rule uses %s context but the host provides no context store", ptContext(r))
		}
	}
	return NewChangeNode(n, c, e), nil
}

// ptContext names the context scope a rule references, for error text.
func ptContext(r ChangeRule) string {
	if r.FieldType != "" {
		return r.FieldType
	}
	return r.ValueType
}

// translateNRRules rewrites c.Rules when they arrived in Node-RED's
// serialization ({t:"set",p:"payload.x",pt:"msg",to:"1",tot:"num"})
// instead of the native {op,field,value} shape. Supported: set/delete/
// move/change with str/num/json/bool/msg/env values, flow/global
// context as source (tot) and target (pt), and the small jsonata
// expression subset (see CompileRuleExpr). JSONata beyond the subset
// and context move/change still fail loudly at deploy.
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
		// pt selects the target scope: "msg" (default) writes into the
		// message, "flow"/"global" write the context store.
		fieldType := ""
		if pt == "flow" || pt == "global" {
			fieldType = pt
		} else if pt != "" && pt != "msg" {
			return fmt.Errorf("change: rule %d targets %q — not supported", i, pt)
		}
		field := nrField(p, pt)
		switch t {
		case "set":
			tot, _ := m["tot"].(string)
			to, _ := m["to"].(string)
			switch tot {
			case "msg":
				translated = append(translated, ChangeRule{Op: "set", Field: field, FieldType: fieldType, FromField: to})
			case "", "str", "num", "json", "bool", "bin", "env", "flow", "global":
				translated = append(translated, ChangeRule{Op: "set", Field: field, FieldType: fieldType, Value: to, ValueType: tot})
			case "jsonata":
				// The expression subset is validated at deploy so
				// unsupported JSONata still fails loud.
				if err := nodebase.CompileRuleExpr(to); err != nil {
					return fmt.Errorf("change: rule %d uses unsupported JSONata expression %q: %w", i, to, err)
				}
				translated = append(translated, ChangeRule{Op: "set", Field: field, FieldType: fieldType, Value: to, ValueType: tot})
			default:
				return fmt.Errorf("change: rule %d uses Node-RED value type %q — not supported", i, tot)
			}
		case "delete":
			translated = append(translated, ChangeRule{Op: "delete", Field: field, FieldType: fieldType})
		case "move":
			if fieldType != "" {
				return fmt.Errorf("change: rule %d moves %s context — not supported", i, fieldType)
			}
			to, _ := m["to"].(string)
			tot, _ := m["tot"].(string)
			if tot != "" && tot != "msg" {
				return fmt.Errorf("change: rule %d move target uses type %q — not supported", i, tot)
			}
			translated = append(translated, ChangeRule{Op: "move", Field: field, Value: nrField(to, "msg")})
		case "change":
			if fieldType != "" {
				return fmt.Errorf("change: rule %d rewrites %s context — not supported", i, fieldType)
			}
			from, _ := m["from"].(string)
			to, _ := m["to"].(string)
			fromt, _ := m["fromt"].(string)
			pattern, err := nrPattern(from, fromt)
			if err != nil {
				return fmt.Errorf("change: rule %d %w", i, err)
			}
			translated = append(translated, ChangeRule{Op: "replace", Field: field, Pattern: pattern, Replacement: to})
		default:
			if t != "" {
				return fmt.Errorf("change: rule %d has unknown Node-RED type %q", i, t)
			}
		}
	}
	c.Rules = translated
	return nil
}

// nrPattern converts a Node-RED change-rule "from" expression into a
// Go regexp pattern. The value may arrive wrapped as /pattern/flags
// (e.g. /foo/i); supported flags map onto Go inline flags. A "fromt"
// of "str" means a literal string rather than a regex; "re"/"" are
// regular expressions. Anything else is rejected.
func nrPattern(from, fromt string) (string, error) {
	switch fromt {
	case "", "re":
	case "str":
		return regexp.QuoteMeta(from), nil
	default:
		return "", fmt.Errorf("uses Node-RED search type %q — not supported", fromt)
	}
	if len(from) > 1 && from[0] == '/' {
		if i := strings.LastIndex(from[1:], "/"); i >= 0 {
			pattern, flags := from[1:1+i], from[1+i+1:]
			var goFlags string
			for _, f := range flags {
				switch f {
				case 'i':
					goFlags += "i"
				case 'm':
					goFlags += "m"
				case 's':
					goFlags += "s"
				case 'g': // always replace-all; flag is implicit
				default:
					return "", fmt.Errorf("uses Node-RED regex flag %q — not supported", string(f))
				}
			}
			if goFlags != "" {
				return "(?" + goFlags + ")" + pattern, nil
			}
			return pattern, nil
		}
	}
	return from, nil
}

// nrField maps a Node-RED rule target (p + pt) onto the native field
// convention: "payload"/"" is the whole payload, "payload.x" is a path
// inside it, other msg.* properties use the "msg." metadata prefix.
// For pt "flow"/"global" the field is the context key verbatim.
func nrField(p, pt string) string {
	if pt == "" {
		pt = "msg"
	}
	switch {
	case pt == "flow" || pt == "global":
		return p // context key, not a message path
	case p == "payload":
		return "payload"
	case strings.HasPrefix(p, "payload."):
		return strings.TrimPrefix(p, "payload.")
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
						"enum": []string{"set", "delete", "move", "replace"},
					},
					"field": map[string]interface{}{
						"type":            "string",
						"title":           "Field",
						"format":          "typedinput",
						"typeField":       "fieldType",
						"typedInputTypes": []string{"msg", "flow", "global"},
					},
					"value": map[string]interface{}{
						"type":            "string",
						"title":           "Value",
						"format":          "typedinput",
						"typeField":       "valueType",
						"typedInputTypes": []string{"str", "num", "bool", "json", "msg", "env", "flow", "global", "jsonata"},
					},
					"pattern": map[string]string{
						"type":  "string",
						"title": "Pattern",
					},
					"replacement": map[string]string{
						"type":  "string",
						"title": "Replacement",
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
	Op    string `json:"op"` // set | delete | move | replace
	Field string `json:"field"`
	// FieldType selects where Field resolves: "" (or "msg") is the
	// message, "flow"/"global" is the context store — Node-RED's pt.
	FieldType string `json:"fieldType,omitempty"`
	Value     string `json:"value"`
	// Pattern and Replacement drive the "replace" op: the field value
	// is stringified, regexp.ReplaceAllString(Pattern, Replacement) is
	// applied ($1/${name} backrefs supported), and the result is
	// written back — Node-RED's t:"change" rule.
	Pattern     string `json:"pattern,omitempty"`
	Replacement string `json:"replacement,omitempty"`
	// ValueType is Node-RED's tot: how Value is interpreted for "set"
	// — "str" (default), "num", "bool", "json", "msg" (read the
	// value from a message property path, same as FromField), "env"
	// (environment variable name), "flow"/"global" (context key), or
	// "jsonata" (the supported expression subset).
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
	config     *ChangeConfig
	host       nodebase.NodeHost
	workflowID string
}

// NewChangeNode creates a new change node. host is optional — without
// it, rules touching flow/global context or env vars resolve to
// absent values.
func NewChangeNode(node nodestore.NodeInterface, config *ChangeConfig, host ...nodebase.NodeHost) *ChangeNode {
	c := &ChangeNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		workflowID: node.GetWorkflowID(),
	}
	if len(host) > 0 {
		c.host = host[0]
	}
	return c
}

// Execute applies each configured rule in order and returns the
// mutated payload
func (n *ChangeNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	for _, rule := range n.config.Rules {
		switch rule.Op {
		case "set":
			n.applySet(input, rule)
		case "delete":
			if rule.FieldType == "flow" || rule.FieldType == "global" {
				n.ruleContext().ContextDelete(rule.FieldType, rule.Field)
				continue
			}
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
		case "replace":
			n.applyReplace(input, rule)
		}
	}
	return input, nil
}

// ruleContext binds the host seam and node identity for context/env
// resolution in this node's rules.
func (n *ChangeNode) ruleContext() *nodebase.RuleContext {
	return &nodebase.RuleContext{Host: n.host, WorkflowID: n.workflowID, NodeID: n.ID()}
}

// applySet handles the "set" op: a "flow"/"global" FieldType writes
// the context store; otherwise the special field "payload"/""
// replaces msg.Payload, "msg.x" writes into Metadata, and any other
// dotted path is set inside the data map.
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
			// A "payload.x" source is a path inside the payload itself —
			// strip the leading "payload." before walking the map.
			value, _ = nodebase.GetPath(m, strings.TrimPrefix(src, "payload."))
		}
	} else {
		rc := n.ruleContext()
		switch rule.ValueType {
		case "env":
			// Environment variable name -> its value; absent resolves
			// to undefined (nil) like a missing context key.
			v, _ := rc.Env(rule.Value)
			value = v
		case "flow", "global":
			v, _ := rc.ContextGet(rule.ValueType, rule.Value)
			value = v
		case "jsonata":
			v, err := nodebase.EvalRuleExpr(rule.Value, rc)
			if err == nil {
				value = v
			}
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

	if rule.FieldType == "flow" || rule.FieldType == "global" {
		n.ruleContext().ContextSet(rule.FieldType, rule.Field, value)
		return
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

// applyReplace handles the "replace" op: reads the current field value
// (non-strings are stringified with %v), applies Pattern via
// regexp.ReplaceAllString with Replacement ($1/${name} backrefs), and
// writes the result back. An invalid pattern or a missing field skips
// the rule, matching the failing-rule convention.
func (n *ChangeNode) applyReplace(input nodestore.MessageInterface, rule ChangeRule) {
	re, err := regexp.Compile(rule.Pattern)
	if err != nil {
		return
	}

	var current interface{}
	if rule.Field == "" || rule.Field == "payload" {
		current = input.GetPayload()
	} else if path, ok := nodebase.MsgPath(rule.Field); ok {
		v, found := nodebase.GetPath(input.GetMetadata(), path)
		if !found {
			return
		}
		current = v
	} else {
		m, ok := input.GetPayload().(map[string]interface{})
		if !ok {
			return
		}
		v, found := nodebase.GetPath(m, rule.Field)
		if !found {
			return
		}
		current = v
	}

	str, ok := current.(string)
	if !ok {
		str = fmt.Sprintf("%v", current)
	}
	replaced := re.ReplaceAllString(str, rule.Replacement)

	if rule.Field == "" || rule.Field == "payload" {
		input.SetPayload(replaced)
		return
	}
	if path, ok := nodebase.MsgPath(rule.Field); ok {
		if input.GetMetadata() == nil {
			input.SetMetadata(map[string]interface{}{})
		}
		nodebase.SetPath(input.GetMetadata(), path, replaced)
		return
	}
	m, ok := input.GetPayload().(map[string]interface{})
	if !ok {
		m = map[string]interface{}{}
		input.SetPayload(m)
	}
	nodebase.SetPath(m, rule.Field, replaced)
}
