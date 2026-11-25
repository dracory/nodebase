// Package switchnode contains the switch node implementation.
package switchnode

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"regexp"
	"strings"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(SwitchConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	// Imported Node-RED flows carry rules in upstream shape
	// ({t:"eq",p:"payload",pt:"msg",v:"1",vt:"num"}) — translate
	// the ops we support; anything else fails loud at deploy.
	if err := translateNRRules(n, c); err != nil {
		return nil, err
	}
	return NewSwitchNode(n, c), nil
}

// nrRuleOps maps Node-RED rule type codes onto native ops. Types with
// no native equivalent (btwn, head, tail, index, hask, jsonata_exp,
// true/false/null/nnull, nempty) are deliberately absent.
var nrRuleOps = map[string]string{
	"eq": "eq", "neq": "neq", "lt": "lt", "lte": "lte",
	"gt": "gt", "gte": "gte", "cont": "contains",
	"regex": "regex", "empty": "empty", "istype": "istype",
}

// translateNRRules rewrites c.Rules in place when they arrived in the
// upstream Node-RED serialization ({t,p,pt,v,vt} keys rather than
// {property,op,value}). An "else" rule sets the otherwise port. Rules
// referencing flow/global context or unsupported types error out.
func translateNRRules(n nodestore.NodeInterface, c *SwitchConfig) error {
	raw, ok := n.GetData()["rules"].([]interface{})
	if !ok {
		return nil
	}
	translated := make([]nodebase.SwitchRule, 0, len(raw))
	for i, r := range raw {
		m, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		if _, hasOp := m["op"]; hasOp {
			continue // already native shape
		}
		t, _ := m["t"].(string)
		if t == "" {
			continue
		}
		if t == "else" {
			c.Otherwise = true
			continue
		}
		op, supported := nrRuleOps[t]
		if !supported {
			return fmt.Errorf("switch: rule %d uses Node-RED type %q which has no equivalent op", i, t)
		}
		if pt, _ := m["pt"].(string); pt != "" && pt != "msg" {
			return fmt.Errorf("switch: rule %d reads %s context — only msg properties are supported", i, pt)
		}
		p, _ := m["p"].(string)
		// msg.payload maps to the payload itself (""); msg.payload.x
		// strips the prefix; anything else (msg.topic, …) resolves via
		// the metadata fallback in EvalSwitchRule.
		p = strings.TrimPrefix(p, "payload.")
		if p == "payload" {
			p = ""
		}
		v, _ := m["v"].(string)
		translated = append(translated, nodebase.SwitchRule{Property: p, Op: op, Value: v})
	}
	if len(translated) > 0 {
		c.Rules = translated
	}
	return nil
}

// init registers the switch node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "switch",
			Name:        "Switch",
			Description: "Routes the message to one of several outputs based on rules",
			Icon:        "code-branch",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the switch node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"rules": map[string]interface{}{
			"type":        "array",
			"title":       "Rules",
			"description": "Ordered list of rules, each {property, op, value}. Property is a dotted path into the payload; rule i routes to output_{i+1}.",
			"items": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"property": map[string]string{
						"type": "string",
					},
					"op": map[string]interface{}{
						"type": "string",
						"enum": []string{"eq", "neq", "gt", "lt", "lte", "gte", "contains", "empty", "regex", "istype"},
					},
					"value": map[string]string{
						"type": "string",
					},
				},
			},
		},
		"mode": map[string]interface{}{
			"type":        "string",
			"title":       "Mode",
			"enum":        []string{"first", "all"},
			"default":     "first",
			"description": "first: route to the first matching output only; all: route to every matching output",
		},
		"otherwise": map[string]interface{}{
			"type":        "boolean",
			"title":       "Otherwise Port",
			"default":     false,
			"description": "Adds a port after the last rule output that receives messages matching no rule",
		},
	},
}

// SwitchConfig holds configuration for the switch node
type SwitchConfig struct {
	Rules []nodebase.SwitchRule `json:"rules"`
	Mode  string                `json:"mode"` // first | all (default "first")
	// Otherwise adds a dedicated port after the last rule output; a
	// message matching no rule is routed there (Node-RED "otherwise").
	Otherwise bool `json:"otherwise"`
}

// SwitchNode routes the message to one of several output ports based on
// rules evaluated against the payload — the Node-RED switch equivalent.
// The node itself is a pass-through; routing happens via SelectPorts.
type SwitchNode struct {
	*nodebase.BaseNode
	config *SwitchConfig
}

// NewSwitchNode creates a new switch node
func NewSwitchNode(node nodestore.NodeInterface, config *SwitchConfig) *SwitchNode {
	return &SwitchNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute passes the input through unchanged — routing is done by the
// host via SelectPorts
func (n *SwitchNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// switchOperators is the accepted op set (mirrors the schema enum).
var switchOperators = map[string]bool{
	"eq": true, "neq": true, "gt": true, "lt": true,
	"lte": true, "gte": true, "contains": true, "empty": true,
	"regex": true, "istype": true,
}

// Validate rejects bad rule configuration at deploy time (project
// policy: unsupported features fail loudly, not silently at message
// time).
func (n *SwitchNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Mode != "" && n.config.Mode != "first" && n.config.Mode != "all" {
		return fmt.Errorf("switch: invalid mode %q", n.config.Mode)
	}
	for i, rule := range n.config.Rules {
		if !switchOperators[rule.Op] {
			return fmt.Errorf("switch: rule %d has unknown op %q", i, rule.Op)
		}
		if rule.Op == "regex" {
			p := strings.TrimPrefix(strings.TrimSuffix(rule.Value, "/"), "/")
			if _, err := regexp.Compile(p); err != nil {
				return fmt.Errorf("switch: rule %d has invalid regex %q: %w", i, rule.Value, err)
			}
		}
	}
	return nil
}

// SelectPorts evaluates the configured rules in order against the input
// payload and returns the matching output port keys. "first" mode stops
// at the first match; "all" returns every matching port. No match returns
// an empty slice — the message dies at this node.
func (n *SwitchNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	var ports []string
	for i, rule := range n.config.Rules {
		if nodebase.EvalSwitchRule(rule, input) {
			ports = append(ports, fmt.Sprintf("output_%d", i+1))
			if n.mode() == "first" {
				break
			}
		}
	}
	if ports == nil {
		// No rule matched: the optional "otherwise" port sits right
		// after the last rule output (Node-RED otherwise handling).
		if n.config.Otherwise {
			return []string{fmt.Sprintf("output_%d", len(n.config.Rules)+1)}
		}
		return []string{}
	}
	return ports
}

// mode returns the configured evaluation mode, defaulting to "first"
func (n *SwitchNode) mode() string {
	if n.config.Mode == "all" {
		return "all"
	}
	return "first"
}
