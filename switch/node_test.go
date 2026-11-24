package switchnode

// Pack-local unit tests for the switch node — port routing modes and
// deploy-time validation.

import (
	"context"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *SwitchConfig) *SwitchNode {
	return NewSwitchNode(nodestore.NewNode("s1", "switch"), cfg)
}

func msg(payload interface{}) nodestore.MessageInterface {
	return nodestore.NewMessage().SetPayload(payload)
}

func TestSwitchFirstMatch(t *testing.T) {
	n := newNode(&SwitchConfig{Rules: []nodebase.SwitchRule{
		{Property: "v", Op: "gt", Value: "5"},
		{Property: "v", Op: "gt", Value: "1"},
	}})
	out, err := n.Execute(context.Background(), msg(map[string]interface{}{"v": 9.0}))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	ports := n.SelectPorts(out, out)
	if len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("first mode should stop at first match: %v", ports)
	}
}

func TestSwitchAllMatches(t *testing.T) {
	n := newNode(&SwitchConfig{Mode: "all", Rules: []nodebase.SwitchRule{
		{Property: "v", Op: "gt", Value: "5"},
		{Property: "v", Op: "gt", Value: "1"},
		{Property: "v", Op: "eq", Value: "999"},
	}})
	ports := n.SelectPorts(msg(map[string]interface{}{"v": 9.0}), nil)
	if len(ports) != 2 || ports[0] != "output_1" || ports[1] != "output_2" {
		t.Fatalf("all mode wrong: %v", ports)
	}
}

func TestSwitchNoMatch(t *testing.T) {
	n := newNode(&SwitchConfig{Rules: []nodebase.SwitchRule{
		{Property: "v", Op: "eq", Value: "1"},
	}})
	if ports := n.SelectPorts(msg(map[string]interface{}{"v": 2.0}), nil); len(ports) != 0 {
		t.Fatalf("no match should yield empty ports: %v", ports)
	}
}

func TestSwitchOtherwise(t *testing.T) {
	n := newNode(&SwitchConfig{Otherwise: true, Rules: []nodebase.SwitchRule{
		{Property: "v", Op: "eq", Value: "1"},
		{Property: "v", Op: "eq", Value: "2"},
	}})
	ports := n.SelectPorts(msg(map[string]interface{}{"v": 99.0}), nil)
	if len(ports) != 1 || ports[0] != "output_3" {
		t.Fatalf("otherwise port wrong: %v", ports)
	}
}

func TestSwitchValidate(t *testing.T) {
	good := newNode(&SwitchConfig{Mode: "all", Rules: []nodebase.SwitchRule{
		{Property: "v", Op: "regex", Value: "^a+$"},
	}})
	if err := good.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := []*SwitchConfig{
		{Mode: "bogus"},
		{Rules: []nodebase.SwitchRule{{Property: "v", Op: "bogus"}}},
		{Rules: []nodebase.SwitchRule{{Property: "v", Op: "regex", Value: "([bad"}}},
	}
	for i, cfg := range cases {
		if err := newNode(cfg).Validate(); err == nil {
			t.Fatalf("case %d: invalid config accepted", i)
		}
	}
}
