// Package uigauge contains the uiGauge dashboard widget: a numeric
// value against a configured min/max range with units. Non-numeric
// payloads push nothing — the retained state keeps the last good
// reading.
package uigauge

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &UIGaugeConfig{Min: 0, Max: 100}
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	if c.Max <= c.Min {
		return nil, fmt.Errorf("uiGauge max (%v) must exceed min (%v)", c.Max, c.Min)
	}
	return NewUIGaugeNode(n, e, c), nil
}

// init registers the uiGauge node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "uiGauge",
			Name:        "UI Gauge",
			Description: "Dashboard gauge: numeric payload on a min/max dial with units",
			Icon:        "tachometer-alt",
			Category:    "Dashboard",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the uiGauge node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": nodebase.UIWithLayout(map[string]interface{}{
		"min": map[string]interface{}{
			"type":    "number",
			"title":   "Min",
			"default": 0,
		},
		"max": map[string]interface{}{
			"type":    "number",
			"title":   "Max",
			"default": 100,
		},
		"units": map[string]interface{}{
			"type":  "string",
			"title": "Units",
		},
	}),
}

// UIGaugeConfig holds the uiGauge node configuration; it is also the
// manifest Config the client renderer consumes.
type UIGaugeConfig struct {
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Units string  `json:"units"`
}

// UIGaugeNode pushes {"value": n} to the dashboard sink.
type UIGaugeNode struct {
	*nodebase.UIWidgetNode
	cfg *UIGaugeConfig
}

// NewUIGaugeNode creates a new uiGauge widget node.
func NewUIGaugeNode(node nodestore.NodeInterface, host nodebase.NodeHost, cfg *UIGaugeConfig) *UIGaugeNode {
	return &UIGaugeNode{UIWidgetNode: nodebase.NewUIWidgetNode(node, host), cfg: cfg}
}

// Execute pushes {"value": n}; non-numeric payloads push nothing (the
// retained state simply keeps the last good reading).
func (n *UIGaugeNode) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	data := in.GetPayload()
	if v, ok := nodebase.UINumberValue(nodebase.UIPayloadValue(data)); ok {
		state := map[string]interface{}{"value": v}
		if ui := nodebase.UIOverrides(data); ui != nil {
			nodebase.UIApplyOverrides(state, ui)
		}
		n.Push(state)
	}
	return in, nil
}
