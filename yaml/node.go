// Package yaml contains the yaml node implementation.
package yaml

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"

	"gopkg.in/yaml.v3"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(YAMLConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewYAMLNode(n, c), nil
}

// init registers the yaml node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "yaml",
			Name:        "YAML",
			Description: "Converts between a YAML string and its object form",
			Icon:        "file-alt",
			Category:    "Parsers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the yaml node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"action": map[string]interface{}{
			"type":    "string",
			"title":   "Action",
			"enum":    []string{"", "str", "obj"},
			"default": "",
			"description": "auto (empty): stringify objects, parse strings; " +
				"str: always convert to a YAML string; obj: always parse a YAML string",
		},
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Field to convert; \"payload\" (default) or a dotted path inside the payload",
			"default":     "payload",
		},
		"outputs": map[string]interface{}{
			"type":        "integer",
			"title":       "Outputs",
			"enum":        []int{1, 2},
			"default":     1,
			"description": "2 adds a second port that receives messages whose YAML failed to parse",
		},
	},
}

// YAMLConfig holds configuration for the yaml node — same shape as the
// json node (yaml.v3 is already vendored).
type YAMLConfig struct {
	// Action selects the conversion: "" (auto: stringify objects, parse
	// strings) | "str" (always stringify) | "obj" (always parse)
	Action string `json:"action"`
	// Property is the message field to convert; "payload" (default)
	// targets msg.Payload, otherwise a dotted path inside the data map
	Property string `json:"property"`
	// Outputs is the node port count; 2 adds a second port that receives
	// messages whose YAML failed to parse (error info in Metadata.error)
	Outputs int `json:"outputs"`
}

// YAMLNode converts payload data between YAML string and object form.
type YAMLNode struct {
	*nodebase.BaseNode
	config *YAMLConfig
	// parseFailed flags the last Execute's outcome so SelectPorts can
	// route the message to the error port (output_2)
	parseFailed bool
}

// NewYAMLNode creates a new yaml node
func NewYAMLNode(node nodestore.NodeInterface, config *YAMLConfig) *YAMLNode {
	return &YAMLNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute reads the configured property, converts it and writes the
// result back. A parse failure either routes to output_2 (when the node
// has two outputs) or aborts the branch with an error status.
func (n *YAMLNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
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
func (n *YAMLNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	if n.parseFailed {
		return []string{"output_2"}
	}
	return []string{"output_1"}
}

// property returns the configured source property (default "payload")
func (n *YAMLNode) property() string {
	if n.config.Property == "" {
		return "payload"
	}
	return n.config.Property
}

// convert applies the configured action to the property value
func (n *YAMLNode) convert(value interface{}) (interface{}, error) {
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
		// js-yaml indents nested content by two spaces; yaml.v3's
		// default is four, so drive the encoder explicitly.
		var buf bytes.Buffer
		enc := yaml.NewEncoder(&buf)
		enc.SetIndent(2)
		if err := enc.Encode(value); err != nil {
			return nil, fmt.Errorf("yaml stringify failed: %w", err)
		}
		if err := enc.Close(); err != nil {
			return nil, fmt.Errorf("yaml stringify failed: %w", err)
		}
		return buf.String(), nil
	case "obj":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("yaml parse failed: property is not a string")
		}
		var v interface{}
		if err := yaml.Unmarshal([]byte(s), &v); err != nil {
			return nil, fmt.Errorf("yaml parse failed: %w", err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("invalid yaml action %q (use \"\", str or obj)", n.config.Action)
	}
}
