package nodebase

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	nodestore "github.com/dracory/nodestore"
)

// This file holds the shared machinery of the dashboard widget nodes
// (the ui* family: uiText, uiGauge, uiChart, uiNotification and the
// input widgets uiButton, uiTextInput, uiForm) — the executor-side
// half of the dashboard contract.
//
// # State push seam
//
// Output widgets turn a computed state map into a dashboard update
// addressed to their own run. Executors see only the NodeHost, never
// the hub: a host that runs a dashboard binds a UIWidgetSink via
// BindUIWidgetSink and the widgets resolve it through PushUIState.
// A push on a host with no bound sink is a no-op — the same "update
// without scope" discipline the old hubByEngine binding enforced.
//
// # Widget state conventions (the update "state" payload)
//
//	uiText:         {"value": "<formatted>"}        (+ optional "label")
//	uiGauge:        {"value": <number>}
//	uiChart:        {"value": <number>}             scalar append, or
//	                {"values": [<number>, ...]}     series replace
//	uiNotification: {"text": "<string>", "level": "info|success|warn|error"}
//
// The display value is the message payload: Data["payload"] when Data
// is a map carrying that key (the inject/input convention), else Data
// itself. All output widgets additionally honor a Data["ui"] map as
// per-message overrides merged over the computed state.

// UIWidgetSink is the host-side bridge a dashboard hub implements to
// receive per-widget state updates from ui* executors. The hub keeps
// retained state and broadcasts to connected clients; the contract is
// deliberately one method so any dashboard implementation can bind.
type UIWidgetSink interface {
	// UpdateWidget stores state for nodeID in workflowID and
	// broadcasts it to that workflow's dashboard clients.
	UpdateWidget(workflowID, nodeID string, state map[string]interface{})
}

// uiSinks binds each host to its dashboard sink. BindUIWidgetSink
// records the binding; widget executors resolve the sink through it.
var uiSinks sync.Map // NodeHost -> UIWidgetSink

// BindUIWidgetSink registers sink as the dashboard state sink for
// host — the host-side equivalent of constructing the dashboard hub.
// It returns an unbind function the caller invokes on hub teardown.
// One sink per host: a second bind replaces the first.
func BindUIWidgetSink(host NodeHost, sink UIWidgetSink) (unbind func()) {
	uiSinks.Store(host, sink)
	return func() { uiSinks.Delete(host) }
}

// PushUIState delivers a widget state update to the dashboard sink
// bound to host. It is a no-op when the host has no bound sink (no
// dashboard mounted) or the workflow has no dashboard scope.
func PushUIState(host NodeHost, workflowID, nodeID string, state map[string]interface{}) {
	if host == nil {
		return
	}
	if s, ok := uiSinks.Load(host); ok {
		s.(UIWidgetSink).UpdateWidget(workflowID, nodeID, state)
	}
}

// UIWidgetNode is the shared executor core for ui* widgets: it
// carries the host reference and the node's workflow id (resolved at
// factory time from the node entity — the NodeFactory signature
// carries no workflow parameter) and turns a computed state map into
// a sink push.
type UIWidgetNode struct {
	*BaseNode
	host       NodeHost
	workflowID string
}

// NewUIWidgetNode creates the shared widget core for node on host.
func NewUIWidgetNode(node nodestore.NodeInterface, host NodeHost) *UIWidgetNode {
	return &UIWidgetNode{
		BaseNode:   NewBaseNode(node),
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Push stores state on the bound sink and broadcasts it to the
// workflow's dashboard clients. No-op when no sink is bound.
func (n *UIWidgetNode) Push(state map[string]interface{}) {
	PushUIState(n.host, n.workflowID, n.ID(), state)
}

// UIPayloadValue extracts the display value from message Data:
// Data["payload"] wins when Data is a map carrying the key (the
// inject/client-input convention); otherwise Data itself is the value.
func UIPayloadValue(data interface{}) interface{} {
	if m, ok := data.(map[string]interface{}); ok {
		if v, present := m["payload"]; present {
			return v
		}
	}
	return data
}

// UIOverrides returns the optional per-message override map carried
// in Data["ui"] (merged over computed widget state by the executor).
func UIOverrides(data interface{}) map[string]interface{} {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}
	ui, _ := m["ui"].(map[string]interface{})
	return ui
}

// UIApplyOverrides merges the Data["ui"] override map into state —
// only keys the state already carries may be overridden, so a message
// cannot invent arbitrary render directives.
func UIApplyOverrides(state map[string]interface{}, ui map[string]interface{}) {
	for k := range state {
		if v, ok := ui[k]; ok {
			state[k] = v
		}
	}
}

// UIFormatValue renders a display value as text: strings pass
// through, numbers print compactly, anything else is JSON-encoded.
func UIFormatValue(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case float32, int, int64, int32, bool:
		return fmt.Sprintf("%v", t)
	default:
		raw, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(raw)
	}
}

// UINumberValue coerces a display value to float64 — numbers pass
// through, numeric strings parse, anything else reports ok=false.
func UINumberValue(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case int32:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	}
	return 0, false
}

// UINumberSlice coerces a display value to a float slice — arrays
// element-wise (non-numeric entries dropped), a scalar to a
// single-element slice.
func UINumberSlice(v interface{}) []float64 {
	var out []float64
	switch t := v.(type) {
	case []interface{}:
		for _, item := range t {
			if f, ok := UINumberValue(item); ok {
				out = append(out, f)
			}
		}
	case []float64:
		out = append(out, t...)
	default:
		if f, ok := UINumberValue(t); ok {
			out = append(out, f)
		}
	}
	return out
}

// UILayoutSchemaFields returns the shared widget layout properties —
// plain string/number fields matching the convention the dashboard
// manifest reads (page/group/label/order; no config nodes).
func UILayoutSchemaFields() map[string]interface{} {
	return map[string]interface{}{
		"page": map[string]interface{}{
			"type":        "string",
			"title":       "Page",
			"default":     "default",
			"description": "Dashboard page the widget renders on",
		},
		"group": map[string]interface{}{
			"type":        "string",
			"title":       "Group",
			"description": "Optional section inside the page",
		},
		"label": map[string]interface{}{
			"type":        "string",
			"title":       "Label",
			"description": "Widget caption; falls back to the node name then id",
		},
		"order": map[string]interface{}{
			"type":        "number",
			"title":       "Order",
			"description": "Optional sort hint within the group",
		},
	}
}

// UIWithLayout returns a schema properties map = layout fields +
// extra type-specific fields.
func UIWithLayout(extra map[string]interface{}) map[string]interface{} {
	props := UILayoutSchemaFields()
	for k, v := range extra {
		props[k] = v
	}
	return props
}
