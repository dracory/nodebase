package aitool

// Pack-local unit tests for the ai-tool node: metadata stamping,
// reference parsing, argument decoding and the llm.Tool declaration
// the agent advertises.

import (
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func TestExecuteStampsCallMetadata(t *testing.T) {
	n := NewToolNode(nodestore.NewNode("t1", Type), &Config{ToolName: "get_weather"}, nil)
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"city": "Paris"})
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	call, _ := out.GetMetadata()[MetaKey].(map[string]interface{})
	if call["name"] != "get_weather" || call["toolNodeId"] != "t1" {
		t.Fatalf("tool-call metadata wrong: %+v", call)
	}
	if _, ok := out.GetPayload().(map[string]interface{}); !ok {
		t.Fatal("payload must pass through untouched")
	}
}

func TestNameDefaultsToNodeID(t *testing.T) {
	n := NewToolNode(nodestore.NewNode("my_tool", Type), &Config{}, nil)
	if n.Name() != "my_tool" {
		t.Fatalf("name wrong: %q", n.Name())
	}
	if err := n.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	bad := NewToolNode(nodestore.NewNode("b", Type), &Config{ToolName: "not a name"}, nil)
	if err := bad.Validate(); err == nil {
		t.Fatal("expected invalid-name error")
	}
}

func TestParseRef(t *testing.T) {
	wf, id, err := ParseRef("wf1/tool1", "wf0")
	if err != nil || wf != "wf1" || id != "tool1" {
		t.Fatalf("cross-workflow ref wrong: %q %q %v", wf, id, err)
	}
	wf, id, err = ParseRef("tool1", "wf0")
	if err != nil || wf != "wf0" || id != "tool1" {
		t.Fatalf("same-workflow ref wrong: %q %q %v", wf, id, err)
	}
	if _, _, err = ParseRef("a/b/c", "wf0"); err == nil {
		t.Fatal("expected malformed-ref error")
	}
	if _, _, err = ParseRef("", "wf0"); err == nil {
		t.Fatal("expected empty-ref error")
	}
}

func TestDecodeArguments(t *testing.T) {
	m, ok := DecodeArguments(`{"a":1}`).(map[string]interface{})
	if !ok || m["a"] != float64(1) {
		t.Fatalf("object args wrong: %v", DecodeArguments(`{"a":1}`))
	}
	if got := DecodeArguments("not json"); got != "not json" {
		t.Fatalf("raw args wrong: %v", got)
	}
	if got := DecodeArguments(""); got == nil {
		t.Fatal("empty args must decode to an empty object")
	}
}

func TestToolForNode(t *testing.T) {
	node := nodestore.NewNode("t1", Type).SetData(map[string]any{
		"toolName":    "get_weather",
		"description": "weather lookup",
		"parameters":  map[string]any{"type": "object"},
	})
	decl, err := ToolForNode(node)
	if err != nil {
		t.Fatalf("ToolForNode failed: %v", err)
	}
	if decl.Name != "get_weather" || decl.Description != "weather lookup" || decl.Parameters["type"] != "object" {
		t.Fatalf("tool declaration wrong: %+v", decl)
	}
	if _, err := ToolForNode(nodestore.NewNode("d", "debug")); err == nil {
		t.Fatal("expected non-ai-tool error")
	}
}
