// Package condition contains the condition node implementation.
package condition

import (
	"context"
	_ "embed"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(ConditionConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewConditionNode(n, c), nil
}

// init registers the condition node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "condition",
			Name:        "Condition",
			Description: "Conditionally executes different branches of the workflow",
			Icon:        "code-branch",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the condition node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"property": map[string]string{
			"type":        "string",
			"title":       "Property",
			"description": "Dotted path into the payload to evaluate",
		},
		"op": map[string]interface{}{
			"type":    "string",
			"title":   "Operator",
			"enum":    []string{"eq", "neq", "gt", "lt", "contains", "empty"},
			"default": "eq",
		},
		"value": map[string]string{
			"type":  "string",
			"title": "Value",
		},
	},
}

// ConditionConfig holds configuration for the condition node — a thin
// two-output (true/false) variant of the switch node kept for backward
// compatibility with existing flows.
type ConditionConfig struct {
	// Property is a dotted path into the payload (same as switch rules)
	Property string `json:"property"`
	// Op is the comparison operator: eq | neq | gt | lt | contains | empty
	Op string `json:"op"`
	// Value is the comparison operand
	Value string `json:"value"`
}

// ConditionNode routes the message to output_1 when the configured
// expression is true and output_2 when it is false — a single-rule
// switch with a dedicated false port.
type ConditionNode struct {
	*nodebase.BaseNode
	config *ConditionConfig
}

// NewConditionNode creates a new condition node
func NewConditionNode(node nodestore.NodeInterface, config *ConditionConfig) *ConditionNode {
	return &ConditionNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute passes the input through unchanged — routing is done by the
// host via SelectPorts
func (n *ConditionNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// SelectPorts evaluates the configured expression against the input
// payload: true → ["output_1"], false → ["output_2"]. Unlike the switch
// node a condition always routes — there is no "no match" case.
func (n *ConditionNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	rule := nodebase.SwitchRule{
		Property: n.config.Property,
		Op:       n.config.Op,
		Value:    n.config.Value,
	}
	if nodebase.EvalSwitchRule(rule, input) {
		return []string{"output_1"}
	}
	return []string{"output_2"}
}
