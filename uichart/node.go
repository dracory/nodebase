// Package uichart contains the uiChart dashboard widget: a live
// line/bar chart of a numeric series. A scalar payload pushes
// {"value": n} (the client appends it to the rolling series); an
// array payload pushes {"values": [...]} (series replace).
package uichart

import (
	"context"
	_ "embed"
	"fmt"

	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &UIChartConfig{Kind: "line"}
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	if c.Kind != "line" && c.Kind != "bar" {
		return nil, fmt.Errorf("uiChart kind %q must be \"line\" or \"bar\"", c.Kind)
	}
	if c.Points < 0 {
		return nil, fmt.Errorf("uiChart points cannot be negative")
	}
	return NewUIChartNode(n, e, c), nil
}

// init registers the uiChart node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "uiChart",
			Name:        "UI Chart",
			Description: "Dashboard live chart: numeric or array payload, line or bar",
			Icon:        "chart-line",
			Category:    "Dashboard",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the uiChart node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": nodebase.UIWithLayout(map[string]interface{}{
		"kind": map[string]interface{}{
			"type":    "string",
			"title":   "Kind",
			"enum":    []string{"line", "bar"},
			"default": "line",
		},
		"points": map[string]interface{}{
			"type":        "number",
			"title":       "Points",
			"default":     50,
			"description": "Rolling series length kept on the client",
		},
	}),
}

// UIChartConfig holds the uiChart node configuration; it is also the
// manifest Config the client renderer consumes.
type UIChartConfig struct {
	// Kind is "line" (default) or "bar".
	Kind string `json:"kind"`
	// Points caps the client-side rolling series length (0 → 50).
	Points int `json:"points"`
}

// UIChartNode pushes chart state to the dashboard sink.
type UIChartNode struct {
	*nodebase.UIWidgetNode
	cfg *UIChartConfig
}

// NewUIChartNode creates a new uiChart widget node.
func NewUIChartNode(node nodestore.NodeInterface, host nodebase.NodeHost, cfg *UIChartConfig) *UIChartNode {
	return &UIChartNode{UIWidgetNode: nodebase.NewUIWidgetNode(node, host), cfg: cfg}
}

// Execute pushes {"value": n} for a scalar payload (the client
// appends it to the rolling series) or {"values": [...]} for an array
// payload (series replace). Non-numeric payloads push nothing.
func (n *UIChartNode) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	data := in.GetPayload()
	v := nodebase.UIPayloadValue(data)
	state := map[string]interface{}{}
	switch v.(type) {
	case []interface{}, []float64:
		vals := nodebase.UINumberSlice(v)
		if len(vals) == 0 {
			return in, nil
		}
		state["values"] = vals
	default:
		f, ok := nodebase.UINumberValue(v)
		if !ok {
			return in, nil
		}
		state["value"] = f
	}
	if ui := nodebase.UIOverrides(data); ui != nil {
		nodebase.UIApplyOverrides(state, ui)
	}
	n.Push(state)
	return in, nil
}
