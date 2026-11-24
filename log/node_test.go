package log

// Pack-local unit tests for the log node — level defaulting and the
// PostRunEvents hook.

import (
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func TestLogPassThrough(t *testing.T) {
	n := NewLogNode(nodestore.NewNode("l1", "log"), &LogConfig{})
	in := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), in)
	if err != nil || out != in {
		t.Fatal("log should pass through")
	}
}

func TestLogEvents(t *testing.T) {
	// default level info
	node := nodestore.NewNode("l2", "log").SetData(map[string]interface{}{"message": "hello"})
	n := NewLogNode(node, &LogConfig{Message: "hello"})
	ev := n.PostRunEvents(node, nodestore.NewMessage())
	if len(ev) != 1 || ev[0].Level != "info" || ev[0].Message != "hello" {
		t.Fatalf("event wrong: %v", ev)
	}
	// configured level
	node2 := nodestore.NewNode("l3", "log").SetData(map[string]interface{}{"message": "oops", "level": "error"})
	n2 := NewLogNode(node2, &LogConfig{Message: "oops", Level: "error"})
	ev2 := n2.PostRunEvents(node2, nodestore.NewMessage())
	if ev2[0].Level != "error" {
		t.Fatalf("level wrong: %v", ev2)
	}
}
