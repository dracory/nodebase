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
	return NewCSVNode(nodestore.NewNode("c1", "csv"), cfg, nil)
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
	// without columns or a header row, keys are generated col1..colN
	row, ok := parts[0].GetPayload().(map[string]interface{})
	if !ok || row["col1"] != "a" || row["col2"] != "b" {
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
	// parseStrings defaults to true (Node-RED semantics): "30" → 30
	if !ok || m["name"] != "alice" || m["age"] != float64(30) {
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
	// hdrout "all" derives a header row from the object keys
	n := newNode(&CSVConfig{HdrOut: "all"})
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
	row, ok := m["doc"].(map[string]interface{})["table"].(map[string]interface{})
	if !ok || row["col1"] != "x" || row["col2"] != "y" {
		t.Fatalf("row written at wrong location: %v", m)
	}
	pm := parts[0].GetMetadata()["parts"].(map[string]interface{})
	if pm["count"] != 1 || pm["type"] != "csv" {
		t.Fatalf("parts metadata wrong: %v", pm)
	}
}

func TestCSVParseWithTemplate(t *testing.T) {
	// temp names the columns; hdrin is not needed for object rows
	n := newNode(&CSVConfig{Temp: "kind,price,origin"})
	msg := nodestore.NewMessage().SetPayload("Apple,100,Canada\n")
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	parts := n.Expand(msg)
	m, ok := parts[0].GetPayload().(map[string]interface{})
	if !ok || m["kind"] != "Apple" || m["price"] != float64(100) || m["origin"] != "Canada" {
		t.Fatalf("template-keyed row wrong: %v", parts[0].GetPayload())
	}
}

func TestCSVParseMult(t *testing.T) {
	// multi "mult": one message whose payload is an array of objects
	n := newNode(&CSVConfig{Multi: "mult"})
	msg := nodestore.NewMessage().SetPayload("a,b\nc,d\n")
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	arr, ok := msg.GetPayload().([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("mult payload wrong: %v", msg.GetPayload())
	}
	row, ok := arr[0].(map[string]interface{})
	if !ok || row["col1"] != "a" || row["col2"] != "b" {
		t.Fatalf("mult row wrong: %v", arr[0])
	}
	// serialize-mode Expand would fan out — mult stays a single message
	if parts := n.Expand(msg); len(parts) != 1 || parts[0] != msg {
		t.Fatal("mult should emit a single message")
	}
}

func TestCSVSerializeObjectWithTemplate(t *testing.T) {
	// temp selects and orders the columns written from an object
	n := newNode(&CSVConfig{Temp: "kind,price"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"kind": "Apple", "price": 100, "origin": "Canada",
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := msg.GetPayload(); got != "Apple,100\n" {
		t.Fatalf("serialized %q", got)
	}
}

func TestCSVSerializeWithMessageColumns(t *testing.T) {
	// msg.columns outranks nothing-configured and selects columns
	n := newNode(&CSVConfig{HdrOut: "all"})
	msg := nodestore.NewMessage().SetPayload([]interface{}{
		map[string]interface{}{"kind": "Apple", "price": 100, "origin": "Canada"},
	})
	msg.SetMetadata(map[string]interface{}{"columns": "kind,price"})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := msg.GetPayload(); got != "kind,price\nApple,100\n" {
		t.Fatalf("serialized %q", got)
	}
}

func TestCSVSerializeHeaderOnceWithReset(t *testing.T) {
	// hdrout "once": header on the first output, re-armed by msg.reset
	n := newNode(&CSVConfig{Temp: "kind,price", HdrOut: "once"})

	m1 := nodestore.NewMessage().SetPayload(map[string]interface{}{"kind": "Apple", "price": 100})
	if _, err := n.Execute(context.Background(), m1); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := m1.GetPayload(); got != "kind,price\nApple,100\n" {
		t.Fatalf("first output %q", got)
	}

	m2 := nodestore.NewMessage().SetPayload(map[string]interface{}{"kind": "Orange", "price": 120})
	if _, err := n.Execute(context.Background(), m2); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := m2.GetPayload(); got != "Orange,120\n" {
		t.Fatalf("second output %q", got)
	}

	m3 := nodestore.NewMessage().SetPayload(map[string]interface{}{"kind": "Banana", "price": 80})
	m3.SetMetadata(map[string]interface{}{"reset": true})
	if _, err := n.Execute(context.Background(), m3); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := m3.GetPayload(); got != "kind,price\nBanana,80\n" {
		t.Fatalf("post-reset output %q", got)
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
