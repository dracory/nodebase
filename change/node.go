// Package change contains the change node implementation.
package change

import (
	"context"
	_ "embed"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(ChangeConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewChangeNode(n, c), nil
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
						"type": "string",
					},
					"value": map[string]string{
						"type": "string",
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
	value := nodebase.ParseJSONOrString(rule.Value)

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
