package change

// Pack-local unit tests for the change node — set/delete/move rules
// including msg.* metadata paths the spec suite does not exercise.

import (
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *ChangeConfig) *ChangeNode {
	return NewChangeNode(nodestore.NewNode("c1", "change"), cfg)
}

func run(t *testing.T, cfg *ChangeConfig, msg nodestore.MessageInterface) nodestore.MessageInterface {
	t.Helper()
	out, err := newNode(cfg).Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	return out
}

func TestChangeSet(t *testing.T) {
	msg := run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "set", Field: "payload", Value: `{"a":1}`},
	}}, nodestore.NewMessage())
	m, ok := msg.GetPayload().(map[string]interface{})
	if !ok || m["a"] != 1.0 {
		t.Fatalf("payload set wrong: %v", msg.GetPayload())
	}
}

func TestChangeSetNestedAndString(t *testing.T) {
	msg := run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "set", Field: "user.name", Value: `"bob"`},
		{Op: "set", Field: "user.tag", Value: "plain"},
	}}, nodestore.NewMessage())
	m := msg.GetPayload().(map[string]interface{})
	u := m["user"].(map[string]interface{})
	if u["name"] != "bob" || u["tag"] != "plain" {
		t.Fatalf("nested set wrong: %v", m)
	}
}

func TestChangeSetMetadata(t *testing.T) {
	msg := run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "set", Field: "msg.statusCode", Value: "404"},
	}}, nodestore.NewMessage())
	if msg.GetMetadata()["statusCode"] != float64(404) {
		t.Fatalf("metadata set wrong: %v", msg.GetMetadata())
	}
}

func TestChangeDelete(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"keep": 1.0, "gone": 2.0,
	}).SetMetadata(map[string]interface{}{"h": "x"})
	msg = run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "delete", Field: "gone"},
		{Op: "delete", Field: "msg.h"},
	}}, msg)
	m := msg.GetPayload().(map[string]interface{})
	if _, ok := m["gone"]; ok {
		t.Fatal("field should be deleted")
	}
	if _, ok := msg.GetMetadata()["h"]; ok {
		t.Fatal("metadata field should be deleted")
	}
}

func TestChangeMove(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"src": "v"})
	msg = run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "move", Field: "src", Value: "dst.deep"},
	}}, msg)
	m := msg.GetPayload().(map[string]interface{})
	if _, ok := m["src"]; ok {
		t.Fatal("src should be removed")
	}
	if got := m["dst"].(map[string]interface{})["deep"]; got != "v" {
		t.Fatalf("move dst wrong: %v", m)
	}
}

func TestChangeMovePayloadToField(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload("scalar")
	msg = run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "move", Field: "payload", Value: "wrap.inner"},
	}}, msg)
	m := msg.GetPayload().(map[string]interface{})
	if got := m["wrap"].(map[string]interface{})["inner"]; got != "scalar" {
		t.Fatalf("payload move wrong: %v", m)
	}
}

func TestChangeMoveNoops(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"a": 1})
	// missing src / same src-dst / empty dst are all no-ops
	out := run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "move", Field: "missing", Value: "x"},
		{Op: "move", Field: "a", Value: "a"},
		{Op: "move", Field: "a", Value: ""},
		{Op: "move", Field: "", Value: "x"},
		{Op: "move", Field: "payload", Value: "payload"},
		{Op: "bogus", Field: "a", Value: "b"},
	}}, msg)
	if out.GetPayload().(map[string]interface{})["a"] != 1 {
		t.Fatal("no-op moves changed the payload")
	}
}

func TestChangeMoveToMetadata(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"tok": "s3"})
	msg = run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "move", Field: "tok", Value: "msg.token"},
	}}, msg)
	if msg.GetMetadata()["token"] != "s3" {
		t.Fatalf("metadata move wrong: %v", msg.GetMetadata())
	}
}
