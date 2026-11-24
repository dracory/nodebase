// Package json contains the json node implementation.
package json

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(JSONConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewJSONNode(n, c), nil
}

// init registers the json node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "json",
			Name:        "JSON",
			Description: "Converts between a JSON string and its object form",
			Icon:        "code",
			Category:    "Parsers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the json node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"action": map[string]interface{}{
			"type":    "string",
			"title":   "Action",
			"enum":    []string{"", "str", "obj"},
			"default": "",
			"description": "auto (empty): stringify objects, parse strings; " +
				"str: always convert to a JSON string; obj: always parse a JSON string",
		},
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Field to convert; \"payload\" (default) or a dotted path inside the payload",
			"default":     "payload",
		},
		"pretty": map[string]interface{}{
			"type":        "boolean",
			"title":       "Pretty Print",
			"description": "Indent stringified output with two spaces",
			"default":     false,
		},
		"outputs": map[string]interface{}{
			"type":        "integer",
			"title":       "Outputs",
			"enum":        []int{1, 2},
			"default":     1,
			"description": "2 adds a second port that receives messages whose JSON failed to parse",
		},
	},
}

// JSONConfig holds configuration for the json node — the Node-RED
// JSON parser equivalent. Converts the configured property between a
// JSON string and its decoded object representation.
type JSONConfig struct {
	// Action selects the conversion: "" (auto: stringify objects, parse
	// strings) | "str" (always stringify) | "obj" (always parse)
	Action string `json:"action"`
	// Property is the message field to convert; "payload" (default)
	// targets msg.Payload, otherwise a dotted path inside the data map
	Property string `json:"property"`
	// Pretty indents stringified output with two spaces
	Pretty bool `json:"pretty"`
	// Outputs is the node port count; 2 adds a second port that receives
	// messages whose JSON failed to parse (error info in Metadata.error)
	Outputs int `json:"outputs"`
}

// JSONNode converts payload data between string and object form.
type JSONNode struct {
	*nodebase.BaseNode
	config *JSONConfig
	// parseFailed flags the last Execute's outcome so SelectPorts can
	// route the message to the error port (output_2)
	parseFailed bool
}

// NewJSONNode creates a new json node
func NewJSONNode(node nodestore.NodeInterface, config *JSONConfig) *JSONNode {
	return &JSONNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute reads the configured property, converts it and writes the
// result back. A parse failure either routes to output_2 (when the node
// has two outputs) or aborts the branch with an error status.
func (n *JSONNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.parseFailed = false

	prop := n.property()
	value, onData := nodebase.JSONSource(input, prop)

	result, err := n.convert(value)
	if err != nil {
		if n.config.Outputs == 2 {
			n.parseFailed = true
			if input.GetMetadata() == nil {
				input.SetMetadata(map[string]interface{}{})
			}
			input.GetMetadata()["error"] = err.Error()
			return input, nil // pass through unchanged on output_2
		}
		return nil, err
	}

	if onData {
		input.SetPayload(result)
	} else {
		m, ok := input.GetPayload().(map[string]interface{})
		if !ok {
			m = map[string]interface{}{}
			input.SetPayload(m)
		}
		nodebase.SetPath(m, prop, result)
	}
	return input, nil
}

// SelectPorts routes the message to output_2 when the last Execute hit
// a parse error, otherwise to output_1.
func (n *JSONNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	if n.parseFailed {
		return []string{"output_2"}
	}
	return []string{"output_1"}
}

// property returns the configured source property (default "payload")
func (n *JSONNode) property() string {
	if n.config.Property == "" {
		return "payload"
	}
	return n.config.Property
}

// convert applies the configured action to the property value
func (n *JSONNode) convert(value interface{}) (interface{}, error) {
	action := n.config.Action
	if action == "" {
		// auto: strings are parsed, everything else is stringified
		if _, isString := value.(string); isString {
			action = "obj"
		} else {
			action = "str"
		}
	}

	switch action {
	case "str":
		var raw []byte
		var err error
		if n.config.Pretty {
			raw, err = json.MarshalIndent(value, "", "  ")
		} else {
			raw, err = json.Marshal(value)
		}
		if err != nil {
			return nil, fmt.Errorf("json stringify failed: %w", err)
		}
		return string(raw), nil
	case "obj":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("json parse failed: property is not a string")
		}
		var v interface{}
		if err := json.Unmarshal([]byte(s), &v); err != nil {
			return nil, fmt.Errorf("json parse failed: %w", err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("invalid json action %q (use \"\", str or obj)", n.config.Action)
	}
}
