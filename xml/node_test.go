package xml

// Pack-local unit tests for the xml node — decode/marshal in both
// directions plus the property-path variants.

import (
	"context"
	"strings"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *XMLConfig) *XMLNode {
	return NewXMLNode(nodestore.NewNode("x1", "xml"), cfg)
}

func TestXMLParse(t *testing.T) {
	n := newNode(&XMLConfig{})
	msg := nodestore.NewMessage().SetPayload(`<user id="7"><name>bob</name></user>`)
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := msg.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", msg.GetPayload())
	}
	user, ok := m["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected user map, got %v", m)
	}
	attrs, _ := user["$"].(map[string]interface{})
	names, _ := user["name"].([]interface{})
	if attrs["id"] != "7" || len(names) != 1 || names[0] != "bob" {
		t.Fatalf("parsed xml wrong: %v", user)
	}
}

// TestXMLParseExplicitArrayFalse covers the msg.options override —
// with explicitArray:false children collapse to scalars.
func TestXMLParseExplicitArrayFalse(t *testing.T) {
	n := newNode(&XMLConfig{})
	msg := nodestore.NewMessage().SetPayload(`<fruit id="100"><kind>Apple</kind></fruit>`)
	msg.SetMetadata(map[string]interface{}{
		"options": map[string]interface{}{"explicitArray": false},
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	fruit := msg.GetPayload().(map[string]interface{})["fruit"].(map[string]interface{})
	attrs, _ := fruit["$"].(map[string]interface{})
	if attrs["id"] != "100" || fruit["kind"] != "Apple" {
		t.Fatalf("explicitArray:false parse wrong: %v", fruit)
	}
}

func TestXMLMarshal(t *testing.T) {
	n := newNode(&XMLConfig{})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"user": map[string]interface{}{"name": "bob"},
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	s, ok := msg.GetPayload().(string)
	if !ok || !strings.Contains(s, "<name>bob</name>") {
		t.Fatalf("marshalled xml wrong: %q", s)
	}
}

func TestXMLMarshalPretty(t *testing.T) {
	n := newNode(&XMLConfig{Pretty: true})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"user": map[string]interface{}{"name": "bob"},
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	s, _ := msg.GetPayload().(string)
	if !strings.Contains(s, "\n") {
		t.Fatalf("pretty output should be indented: %q", s)
	}
}

func TestXMLErrors(t *testing.T) {
	n := newNode(&XMLConfig{})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("<unclosed")); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestXMLPropertyPath(t *testing.T) {
	n := newNode(&XMLConfig{Property: "doc.raw"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"doc": map[string]interface{}{"raw": `<a><b>1</b></a>`},
	})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m := msg.GetPayload().(map[string]interface{})
	converted, ok := m["doc"].(map[string]interface{})["raw"].(map[string]interface{})
	if !ok {
		t.Fatalf("converted value at wrong location: %v", m)
	}
	if _, ok := converted["a"]; !ok {
		t.Fatalf("expected parsed map, got %v", converted)
	}
}
