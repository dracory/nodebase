package yaml

// Pack-local unit tests for the yaml node — conversion actions and
// error-port routing the noderun spec suite does not reach.

import (
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *YAMLConfig) *YAMLNode {
	return NewYAMLNode(nodestore.NewNode("y1", "yaml"), cfg)
}

func TestYAMLAuto(t *testing.T) {
	n := newNode(&YAMLConfig{})
	// object → string
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"a": 1.0})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	s, ok := msg.GetPayload().(string)
	if !ok || s == "" {
		t.Fatalf("expected yaml string, got %v", msg.GetPayload())
	}
	// string → object
	msg2 := nodestore.NewMessage().SetPayload("a: 1\n")
	if _, err := n.Execute(context.Background(), msg2); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := msg2.GetPayload().(map[string]interface{})
	if !ok || m["a"] != 1 {
		t.Fatalf("expected map, got %v", msg2.GetPayload())
	}
}

func TestYAMLForcedActions(t *testing.T) {
	n := newNode(&YAMLConfig{Action: "str"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"b": true})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if _, ok := msg.GetPayload().(string); !ok {
		t.Fatal("str action should produce a string")
	}

	n2 := newNode(&YAMLConfig{Action: "obj"})
	msg2 := nodestore.NewMessage().SetPayload("x: [1,2]\n")
	if _, err := n2.Execute(context.Background(), msg2); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m := msg2.GetPayload().(map[string]interface{})
	if arr, ok := m["x"].([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("parsed yaml wrong: %v", m)
	}
}

func TestYAMLErrors(t *testing.T) {
	// invalid action
	n := newNode(&YAMLConfig{Action: "bogus"})
	msg := nodestore.NewMessage().SetPayload("a")
	if _, err := n.Execute(context.Background(), msg); err == nil {
		t.Fatal("expected invalid-action error")
	}
	// obj action on non-string
	n2 := newNode(&YAMLConfig{Action: "obj"})
	msg2 := nodestore.NewMessage().SetPayload(42)
	if _, err := n2.Execute(context.Background(), msg2); err == nil {
		t.Fatal("expected not-a-string error")
	}
	// unparseable yaml
	msg3 := nodestore.NewMessage().SetPayload("{bad: [yaml")
	if _, err := n2.Execute(context.Background(), msg3); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestYAMLErrorPort(t *testing.T) {
	n := newNode(&YAMLConfig{Action: "obj", Outputs: 2})
	msg := nodestore.NewMessage().SetPayload("{bad: [yaml")
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if ports := n.SelectPorts(msg, out); len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("parse failure should route to output_2, got %v", ports)
	}
	if msg.GetMetadata()["error"] == nil {
		t.Fatal("error metadata should be set")
	}
	// success routes to output_1
	msg2 := nodestore.NewMessage().SetPayload("a: 1\n")
	out2, err := n.Execute(context.Background(), msg2)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if ports := n.SelectPorts(msg2, out2); ports[0] != "output_1" {
		t.Fatalf("success should route to output_1, got %v", ports)
	}
}

func TestYAMLPropertyPath(t *testing.T) {
	n := newNode(&YAMLConfig{Property: "doc.raw"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"doc": map[string]interface{}{"raw": "k: v\n"},
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m := msg.GetPayload().(map[string]interface{})
	converted := m["doc"].(map[string]interface{})["raw"].(map[string]interface{})
	if converted["k"] != "v" {
		t.Fatalf("converted value at wrong location: %v", m)
	}
}
