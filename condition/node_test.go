package condition

// Pack-local unit tests for the condition node — the true/false port
// routing delegated to the shared switch-rule machinery.

import (
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *ConditionConfig) *ConditionNode {
	return NewConditionNode(nodestore.NewNode("c1", "condition"), cfg)
}

func TestConditionPorts(t *testing.T) {
	n := newNode(&ConditionConfig{Property: "v", Op: "eq", Value: "1"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"v": 1.0})
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if ports := n.SelectPorts(msg, out); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("true should route to output_1, got %v", ports)
	}

	msg2 := nodestore.NewMessage().SetPayload(map[string]interface{}{"v": 2.0})
	if ports := n.SelectPorts(msg2, msg2); ports[0] != "output_2" {
		t.Fatalf("false should route to output_2, got %v", ports)
	}
}

func TestConditionOps(t *testing.T) {
	cases := []struct {
		op, value string
		payload   interface{}
		port      string
	}{
		{"neq", "1", map[string]interface{}{"v": 2.0}, "output_1"},
		{"gt", "1", map[string]interface{}{"v": 2.0}, "output_1"},
		{"lt", "1", map[string]interface{}{"v": 2.0}, "output_2"},
		{"contains", "ell", map[string]interface{}{"v": "hello"}, "output_1"},
		{"empty", "", map[string]interface{}{"v": ""}, "output_1"},
		{"bogus-op", "1", map[string]interface{}{"v": 1.0}, "output_2"},
	}
	for _, c := range cases {
		n := newNode(&ConditionConfig{Property: "v", Op: c.op, Value: c.value})
		msg := nodestore.NewMessage().SetPayload(c.payload)
		if ports := n.SelectPorts(msg, msg); ports[0] != c.port {
			t.Fatalf("op %s: got %v want %s", c.op, ports, c.port)
		}
	}
}
