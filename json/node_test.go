package json

// Pack-local unit tests for the json node — conversion actions, pretty
// output and error-port routing.

import (
	"context"
	"strings"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *JSONConfig) *JSONNode {
	return NewJSONNode(nodestore.NewNode("j1", "json"), cfg)
}

func TestJSONAuto(t *testing.T) {
	n := newNode(&JSONConfig{})
	// string → object
	msg := nodestore.NewMessage().SetPayload(`{"a":1}`)
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := msg.GetPayload().(map[string]interface{})
	if !ok || m["a"] != 1.0 {
		t.Fatalf("expected map, got %v", msg.GetPayload())
	}
	// object → string
	msg2 := nodestore.NewMessage().SetPayload(map[string]interface{}{"a": 1.0})
	if _, err := n.Execute(context.Background(), msg2); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := msg2.GetPayload(); got != `{"a":1}` {
		t.Fatalf("stringify got %v", got)
	}
}

func TestJSONForcedAndPretty(t *testing.T) {
	n := newNode(&JSONConfig{Action: "str", Pretty: true})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"a": 1.0})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	s, ok := msg.GetPayload().(string)
	if !ok || !strings.Contains(s, "\n") {
		t.Fatalf("pretty output should be indented: %q", s)
	}

	n2 := newNode(&JSONConfig{Action: "obj"})
	msg2 := nodestore.NewMessage().SetPayload(`[1,2]`)
	if _, err := n2.Execute(context.Background(), msg2); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if arr, ok := msg2.GetPayload().([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("expected array, got %v", msg2.GetPayload())
	}
}

func TestJSONErrors(t *testing.T) {
	n := newNode(&JSONConfig{Action: "bogus"})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("expected invalid-action error")
	}
	n2 := newNode(&JSONConfig{Action: "obj"})
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload(5)); err == nil {
		t.Fatal("expected not-a-string error")
	}
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("{bad")); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestJSONErrorPort(t *testing.T) {
	n := newNode(&JSONConfig{Outputs: 2})
	msg := nodestore.NewMessage().SetPayload("{bad")
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
}

func TestJSONPropertyPath(t *testing.T) {
	n := newNode(&JSONConfig{Property: "doc.raw"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"doc": map[string]interface{}{"raw": `{"k":"v"}`},
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
