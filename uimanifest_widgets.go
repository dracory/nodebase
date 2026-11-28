package nodebase

// Manifest describers for the 7 core dashboard widget types (uiText,
// uiGauge, uiChart, uiNotification, uiButton, uiTextInput, uiForm).
// The node TYPES self-register from their ui*/ folders; the
// describers live here beside the registry — the same split
// flowdashboard's register.go used — so adding a widget type in this
// package is describer + schema, and external widget packs register
// their own via RegisterUIWidget.
//
// # Manifest config shapes
//
//	uiText:         {} (layout only)
//	uiGauge:        {"min": <num>, "max": <num>, "units": "<string>"}
//	uiChart:        {"kind": "line"|"bar", "points": <num>}
//	uiNotification: {} (layout only)
//	uiButton:       {"payload": <any>}
//	uiTextInput:    {"placeholder": "<string>"}
//	uiForm:         {"fields": [{"name","label","type","options","value"}...]}
//	                 type ∈ text|number|checkbox|select; options for select.

import (
	"encoding/json"
	"strconv"
	"strings"

	nodestore "github.com/dracory/nodestore"
)

func init() {
	_ = RegisterUIWidget("uiText", func(node nodestore.NodeInterface) UIWidgetManifest {
		return UIWidgetLayout(node)
	})
	_ = RegisterUIWidget("uiGauge", describeUIGauge)
	_ = RegisterUIWidget("uiChart", describeUIChart)
	_ = RegisterUIWidget("uiNotification", func(node nodestore.NodeInterface) UIWidgetManifest {
		return UIWidgetLayout(node)
	})
	_ = RegisterUIWidget("uiButton", describeUIButton)
	_ = RegisterUIWidget("uiTextInput", describeUITextInput)
	_ = RegisterUIWidget("uiForm", describeUIForm)
}

// describeUIGauge manifest config: {"min","max","units"} — defaults
// 0..100 mirror the executor's uiGaugeConfig defaults.
func describeUIGauge(node nodestore.NodeInterface) UIWidgetManifest {
	w := UIWidgetLayout(node)
	data := node.GetData()
	min, max := 0.0, 100.0
	if v, ok := UINumberValue(data["min"]); ok {
		min = v
	}
	if v, ok := UINumberValue(data["max"]); ok {
		max = v
	}
	units, _ := data["units"].(string)
	w.Config = map[string]interface{}{
		"min": min, "max": max, "units": units,
	}
	return w
}

// describeUIChart manifest config: {"kind","points"} — kind defaults
// "line", points defaults to the client-side rolling cap of 50.
func describeUIChart(node nodestore.NodeInterface) UIWidgetManifest {
	w := UIWidgetLayout(node)
	data := node.GetData()
	kind := "line"
	if v, ok := data["kind"].(string); ok && v != "" {
		kind = v
	}
	points := 50
	if v, ok := UINumberValue(data["points"]); ok && v > 0 {
		points = int(v)
	}
	w.Config = map[string]interface{}{
		"kind": kind, "points": points,
	}
	return w
}

// defaultUIButtonPayload is emitted when uiButton has no configured
// payload — Node-RED ui_button sends its configured payload; the engine's
// editor default is "clicked" so a freshly dropped button does
// something observable.
const defaultUIButtonPayload = "clicked"

// describeUIButton manifest config: {"payload": <verbatim config
// value>}. Any JSON scalar survives verbatim (the client emits it
// as-is).
func describeUIButton(node nodestore.NodeInterface) UIWidgetManifest {
	w := UIWidgetLayout(node)
	payload, present := node.GetData()["payload"]
	if !present || payload == nil {
		payload = defaultUIButtonPayload
	}
	w.Config = map[string]interface{}{"payload": payload}
	return w
}

// describeUITextInput manifest config: {"placeholder": "<string>"}.
func describeUITextInput(node nodestore.NodeInterface) UIWidgetManifest {
	w := UIWidgetLayout(node)
	placeholder, _ := node.GetData()["placeholder"].(string)
	w.Config = map[string]interface{}{"placeholder": placeholder}
	return w
}

// UIFormField is one normalized form field in the uiForm manifest
// config; the client renderer generates a control per entry.
type UIFormField struct {
	// Name is the msg.payload object key (required — nameless
	// entries are dropped by the describer).
	Name string `json:"name"`
	// Label is the control caption; falls back to Name.
	Label string `json:"label,omitempty"`
	// Type is text (default) | number | checkbox | select.
	Type string `json:"type"`
	// Options lists the select choices (select only).
	Options []string `json:"options,omitempty"`
	// Value is the initial/default control value.
	Value interface{} `json:"value,omitempty"`
}

// describeUIForm manifest config: {"fields": [normalized
// UIFormField]}. Fields arrive as data["fields"] ([]interface{} of
// maps from JSON authoring); anything malformed is coerced or dropped
// so the client always gets a clean array.
func describeUIForm(node nodestore.NodeInterface) UIWidgetManifest {
	w := UIWidgetLayout(node)
	w.Config = map[string]interface{}{"fields": UIFormFields(node.GetData())}
	return w
}

// UIFormFields normalizes node data["fields"] into a typed slice —
// shared by the manifest describer and kept tolerant of hand-edited
// node JSON.
func UIFormFields(data map[string]interface{}) []UIFormField {
	raw, _ := data["fields"].([]interface{})
	fields := make([]UIFormField, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		if name == "" {
			continue
		}
		f := UIFormField{Name: name}
		if v, ok := m["label"].(string); ok {
			f.Label = v
		}
		switch t, _ := m["type"].(string); t {
		case "number", "checkbox", "select":
			f.Type = t
		default:
			f.Type = "text"
		}
		switch opts := m["options"].(type) {
		case string:
			// The schema-driven editor writes select options as a
			// comma-separated list; arrays remain the canonical JSON form.
			for _, part := range strings.Split(opts, ",") {
				if s := strings.TrimSpace(part); s != "" {
					f.Options = append(f.Options, s)
				}
			}
		case []string:
			f.Options = append(f.Options, opts...)
		case []interface{}:
			for _, o := range opts {
				if s, ok := o.(string); ok {
					f.Options = append(f.Options, s)
				}
			}
		}
		if v, present := m["value"]; present {
			f.Value = uiFormTypedValue(v, m["valueType"])
		}
		fields = append(fields, f)
	}
	return fields
}

// uiFormTypedValue interprets the schema editor's typed-input value.
// Rows without an explicit valueType keep their JSON value verbatim so
// legacy hand-authored configurations are unchanged.
func uiFormTypedValue(value interface{}, valueType interface{}) interface{} {
	typed, _ := valueType.(string)
	if typed == "" || typed == "str" {
		return value
	}
	text, _ := value.(string)
	switch typed {
	case "num":
		if n, err := strconv.ParseFloat(text, 64); err == nil {
			return n
		}
	case "bool":
		if b, err := strconv.ParseBool(text); err == nil {
			return b
		}
	case "json":
		var decoded interface{}
		if err := json.Unmarshal([]byte(text), &decoded); err == nil {
			return decoded
		}
	}
	return value
}
