package template

// Pack-local unit tests for the template node — json/text output
// formats and the error paths.

import (
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *TemplateConfig) *TemplateNode {
	return NewTemplateNode(nodestore.NewNode("t1", "template"), cfg)
}

func TestTemplateJSON(t *testing.T) {
	n := newNode(&TemplateConfig{Template: `{"echo": "{{.name}}"}`})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"name": "bob"})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := msg.GetPayload().(map[string]interface{})
	if !ok || m["echo"] != "bob" {
		t.Fatalf("rendered payload wrong: %v", msg.GetPayload())
	}
}

func TestTemplateText(t *testing.T) {
	n := newNode(&TemplateConfig{Template: `hi {{.name}}`, Format: "text"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"name": "bob"})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := msg.GetPayload(); got != "hi bob" {
		t.Fatalf("text output wrong: %v", got)
	}
}

func TestTemplateErrors(t *testing.T) {
	// parse error
	n := newNode(&TemplateConfig{Template: `{{.oops`})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{})); err == nil {
		t.Fatal("expected template parse error")
	}
	// render error: missing key on map with missingkey=error? default is <no value> — use a function call on nil
	n2 := newNode(&TemplateConfig{Template: `{{call .name}}`})
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{"name": "x"})); err == nil {
		t.Fatal("expected template execute error")
	}
	// json output invalid
	n3 := newNode(&TemplateConfig{Template: `not json`})
	if _, err := n3.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{})); err == nil {
		t.Fatal("expected invalid-JSON error")
	}
}
