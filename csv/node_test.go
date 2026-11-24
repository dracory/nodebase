package csv

// Pack-local unit tests for the csv node: parsing, headers, serializing
// and Expand fan-out — exercised directly (no engine) since the
// noderun spec suite only reaches the happy paths.

import (
	"context"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *CSVConfig) *CSVNode {
	return NewCSVNode(nodestore.NewNode("c1", "csv"), cfg)
}

func TestCSVParseRows(t *testing.T) {
	n := newNode(&CSVConfig{})
	msg := nodestore.NewMessage().SetPayload("a,b\nc,d\n")
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	parts := n.Expand(out)
	if len(parts) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(parts))
	}
	row, ok := parts[0].GetPayload().([]string)
	if !ok || row[0] != "a" || row[1] != "b" {
		t.Fatalf("row payload wrong: %v", parts[0].GetPayload())
	}
	pm := parts[1].GetMetadata()["parts"].(map[string]interface{})
	if pm["index"] != 1 || pm["count"] != 2 || pm["type"] != "csv" {
		t.Fatalf("parts metadata wrong: %v", pm)
	}
}

func TestCSVParseWithHeaders(t *testing.T) {
	n := newNode(&CSVConfig{Headers: true})
	msg := nodestore.NewMessage().SetPayload("name,age\nalice,30\n")
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	parts := n.Expand(msg)
	if len(parts) != 1 {
		t.Fatalf("expected 1 row, got %d", len(parts))
	}
	m, ok := parts[0].GetPayload().(map[string]interface{})
	if !ok || m["name"] != "alice" || m["age"] != "30" {
		t.Fatalf("header-keyed row wrong: %v", parts[0].GetPayload())
	}
}

func TestCSVSerialize(t *testing.T) {
	n := newNode(&CSVConfig{})
	msg := nodestore.NewMessage().SetPayload([]interface{}{
		[]interface{}{"a", "b"},
		[]string{"c", "d"},
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := msg.GetPayload(); got != "a,b\nc,d\n" {
		t.Fatalf("serialized %q", got)
	}
	// serialize mode: Expand returns the single output
	if parts := n.Expand(msg); len(parts) != 1 || parts[0] != msg {
		t.Fatal("serialize mode should emit a single message")
	}
}

func TestCSVSerializeWithHeaders(t *testing.T) {
	n := newNode(&CSVConfig{Headers: true})
	msg := nodestore.NewMessage().SetPayload([]interface{}{
		map[string]interface{}{"name": "alice", "age": "30"},
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	got, ok := msg.GetPayload().(string)
	if !ok || got != "age,name\n30,alice\n" {
		t.Fatalf("serialized with headers %q", got)
	}
}

func TestCSVPropertyPath(t *testing.T) {
	n := newNode(&CSVConfig{Property: "doc.table"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"doc": map[string]interface{}{"table": "x,y\n"},
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	parts := n.Expand(msg)
	if len(parts) != 1 {
		t.Fatalf("expected 1 row, got %d", len(parts))
	}
	m := parts[0].GetPayload().(map[string]interface{})
	row, ok := m["doc"].(map[string]interface{})["table"].([]string)
	if !ok || row[0] != "x" {
		t.Fatalf("row written at wrong location: %v", m)
	}
	pm := parts[0].GetMetadata()["parts"].(map[string]interface{})
	if pm["count"] != 1 || pm["type"] != "csv" {
		t.Fatalf("parts metadata wrong: %v", pm)
	}
}

func TestCSVErrors(t *testing.T) {
	n := newNode(&CSVConfig{})
	// unsupported payload type
	msg := nodestore.NewMessage().SetPayload(42)
	if _, err := n.Execute(context.Background(), msg); err == nil {
		t.Fatal("expected error for numeric payload")
	}
	// malformed csv (unclosed quote)
	msg2 := nodestore.NewMessage().SetPayload("\"unclosed")
	if _, err := n.Execute(context.Background(), msg2); err == nil {
		t.Fatal("expected parse error")
	}
	// unsupported row type on serialize
	msg3 := nodestore.NewMessage().SetPayload([]interface{}{42})
	if _, err := n.Execute(context.Background(), msg3); err == nil {
		t.Fatal("expected serialize error for scalar row")
	}
}

func TestCSVEmptyTable(t *testing.T) {
	// header-only input yields zero data rows — nothing to emit
	n := newNode(&CSVConfig{Headers: true})
	msg := nodestore.NewMessage().SetPayload("a,b\n")
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if parts := n.Expand(msg); parts != nil {
		t.Fatal("empty table should emit nothing")
	}
}

func TestCSVConfigDecode(t *testing.T) {
	reg, ok := nodebase.RegisteredNode("csv")
	if !ok {
		t.Fatal("csv not registered")
	}
	executor, err := reg.Factory(nodestore.NewNode("c2", "csv").SetData(map[string]interface{}{
		"property": "doc.t",
		"headers":  true,
	}), nil)
	if err != nil {
		t.Fatalf("factory failed: %v", err)
	}
	node, ok := executor.(*CSVNode)
	if !ok {
		t.Fatalf("expected *CSVNode, got %T", executor)
	}
	if node.config.Property != "doc.t" || !node.config.Headers {
		t.Fatalf("config decoded incorrectly: %+v", node.config)
	}
}
