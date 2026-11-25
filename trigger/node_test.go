package trigger

// Internal coverage ported from flowrunner's node_trigger_test.go
// (config field and triggerValue are unexported).

import (
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// TestTriggerNodeConfigDecode verifies the executor is built from node
// Data with the documented field names.
func TestTriggerNodeConfigDecode(t *testing.T) {
	reg, ok := nodebase.RegisteredNode("trigger")
	if !ok {
		t.Fatal("trigger type not registered")
	}
	executor, err := reg.Factory(nodestore.NewNode("trg", "trigger").SetData(map[string]interface{}{
		"op1":           "1",
		"op1type":       "num",
		"op2":           "0",
		"op2type":       "num",
		"duration":      2.0,
		"units":         "s",
		"extend":        true,
		"overrideDelay": true,
		"outputs":       1,
		"bytopic":       true,
	}), nil)
	if err != nil {
		t.Fatalf("executorForNode failed: %v", err)
	}
	trg, ok2 := executor.(*TriggerNode)
	if !ok2 {
		t.Fatalf("expected *TriggerNode, got %T", executor)
	}
	if trg.config.Op1 != "1" || trg.config.Op1Type != "num" || trg.config.Op2 != "0" ||
		trg.config.Duration != 2.0 || trg.config.Units != "s" || !trg.config.Extend ||
		!trg.config.OverrideDelay || trg.config.Outputs != 1 || trg.config.ByTopic != TopicScope(true) {
		t.Fatalf("config decoded incorrectly: %+v", trg.config)
	}
}

// TestTriggerValue covers the op value type parsing.
func TestTriggerValue(t *testing.T) {
	input := nodestore.NewMessage().SetPayload(map[string]interface{}{"x": 1})
	cases := []struct {
		kind, raw string
		want      interface{}
	}{
		{"str", "abc", "abc"},
		{"", "abc", "abc"},
		{"num", "3.5", 3.5},
		{"num", "notanum", float64(0)},
		{"bool", "true", true},
		{"bool", "false", false},
		{"json", `{"a":1}`, map[string]interface{}{"a": float64(1)}},
	}
	for _, c := range cases {
		got := triggerValue(c.kind, c.raw, input)
		switch want := c.want.(type) {
		case map[string]interface{}:
			m, ok := got.(map[string]interface{})
			if !ok || m["a"] != want["a"] {
				t.Fatalf("triggerValue(%q,%q)=%v want %v", c.kind, c.raw, got, c.want)
			}
		default:
			if got != c.want {
				t.Fatalf("triggerValue(%q,%q)=%v want %v", c.kind, c.raw, got, c.want)
			}
		}
	}
	if got := triggerValue("payload", "", input); got == nil {
		t.Fatal("payload type should pass input.GetPayload() through")
	}
	if got := triggerValue("payload", "", nil); got != nil {
		t.Fatal("payload type with no input should yield nil")
	}
}
