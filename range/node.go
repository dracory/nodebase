// Package rangenode contains the range node implementation.
package rangenode

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(RangeConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewRangeNode(n, c), nil
}

// init registers the range node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "range",
			Name:        "Range",
			Description: "Maps a numeric property from an input range to an output range",
			Icon:        "exchange-alt",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the range node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"action": map[string]interface{}{
			"type":    "string",
			"title":   "Action",
			"enum":    []string{"scale", "drop", "clamp", "roll"},
			"default": "scale",
			"description": "scale: extrapolate freely; drop: discard out-of-range messages; " +
				"clamp: clamp to the input range; roll: wrap into the input range",
		},
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Field to map; \"payload\" (default) or a dotted path inside the payload",
			"default":     "payload",
		},
		"minin": map[string]interface{}{
			"type":        "number",
			"title":       "Input Min",
			"description": "Lower bound of the input range",
			"default":     0,
		},
		"maxin": map[string]interface{}{
			"type":        "number",
			"title":       "Input Max",
			"description": "Upper bound of the input range",
			"default":     100,
		},
		"minout": map[string]interface{}{
			"type":        "number",
			"title":       "Output Min",
			"description": "Lower bound of the output range",
			"default":     0,
		},
		"maxout": map[string]interface{}{
			"type":        "number",
			"title":       "Output Max",
			"description": "Upper bound of the output range",
			"default":     1,
		},
		"round": map[string]interface{}{
			"type":        "boolean",
			"title":       "Round Result",
			"description": "Round the scaled value to the nearest integer",
			"default":     false,
		},
	},
}

// RangeConfig holds configuration for the range node — the Node-RED
// range equivalent. MinIn/MaxIn define the input range, MinOut/MaxOut
// the output range the value is linearly mapped to.
type RangeConfig struct {
	// Action is "scale" (default, extrapolate freely), "drop"
	// (discard out-of-range messages), "clamp" (clamp to the input
	// range) or "roll" (wrap into the input range)
	Action string `json:"action"`
	// Property is the dotted path into the payload; "payload" (the
	// default) targets the whole payload
	Property string  `json:"property"`
	MinIn    float64 `json:"minin"`
	MaxIn    float64 `json:"maxin"`
	MinOut   float64 `json:"minout"`
	MaxOut   float64 `json:"maxout"`
	// Round rounds the scaled result to the nearest integer
	Round bool `json:"round"`
}

// RangeNode linearly maps a numeric payload property from an input
// range to an output range, with an optional out-of-range action
// applied to the input range before scaling.
type RangeNode struct {
	*nodebase.BaseNode
	config *RangeConfig
}

// NewRangeNode creates a new range node
func NewRangeNode(node nodestore.NodeInterface, config *RangeConfig) *RangeNode {
	return &RangeNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute reads the configured property, applies the out-of-range
// action, scales the value and writes it back. A non-numeric value or a
// value dropped by the "drop" action ends the message here (nil output);
// a missing property passes the message through unchanged.
func (n *RangeNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	value, found := nodebase.RangeProperty(input, n.property())
	if !found {
		return input, nil
	}

	num, ok := nodebase.ParseNumeric(value)
	if !ok {
		// Node-RED reports this as a node error (catchable), not a drop
		return nil, fmt.Errorf("range: non-numeric value %v", value)
	}

	// Upstream rounds the output bounds inward when round is set
	// (maxout floored, minout ceiled) BEFORE scaling — so a clamped
	// maxin maps to an integer.
	minOut, maxOut := n.config.MinOut, n.config.MaxOut
	if n.config.Round {
		maxOut = math.Floor(maxOut)
		minOut = math.Ceil(minOut)
	}

	// The action applies to the input range, before scaling.
	span := n.config.MaxIn - n.config.MinIn
	switch n.config.Action {
	case "drop":
		if num < n.config.MinIn || num > n.config.MaxIn {
			return nil, nil
		}
	case "clamp":
		if num < n.config.MinIn {
			num = n.config.MinIn
		} else if num > n.config.MaxIn {
			num = n.config.MaxIn
		}
	case "roll":
		if span != 0 {
			num = math.Mod(math.Mod(num-n.config.MinIn, span)+span, span) + n.config.MinIn
		}
		// "scale" (default) extrapolates freely — no check
	}

	var out float64
	if span == 0 {
		out = minOut
	} else {
		out = (num-n.config.MinIn)/span*(maxOut-minOut) + minOut
	}
	if n.config.Round {
		out = math.Round(out)
	}

	setRangeProperty(input, n.property(), out)
	return input, nil
}

// property returns the configured property path (default "payload")
func (n *RangeNode) property() string {
	if n.config.Property == "" {
		return "payload"
	}
	return n.config.Property
}

// setRangeProperty writes the scaled value back: "payload" replaces the
// whole Data, other paths set inside the data map (creating it if needed).
func setRangeProperty(input nodestore.MessageInterface, property string, value interface{}) {
	if property == "payload" {
		input.SetPayload(value)
		return
	}
	m, ok := input.GetPayload().(map[string]interface{})
	if !ok {
		m = map[string]interface{}{}
		input.SetPayload(m)
	}
	nodebase.SetPath(m, property, value)
}
