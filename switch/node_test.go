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

// TestSwitchElse covers the rule-level "else" catch-all: the else
// port claims the message only when no ordinary rule matched.
func TestSwitchElse(t *testing.T) {
	n := newNode(&SwitchConfig{Rules: []nodebase.SwitchRule{
		{Property: "v", Op: "eq", Value: "1"},
		{Op: "else"},
	}})
	if ports := n.SelectPorts(msg(map[string]interface{}{"v": 1.0}), nil); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("matching rule should win over else: %v", ports)
	}
	if ports := n.SelectPorts(msg(map[string]interface{}{"v": 9.0}), nil); len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("else should catch unmatched messages: %v", ports)
	}
	// "all" mode: else still yields only when nothing else matched.
	all := newNode(&SwitchConfig{Mode: "all", Rules: []nodebase.SwitchRule{
		{Op: "else"},
		{Property: "v", Op: "gt", Value: "5"},
	}})
	if ports := all.SelectPorts(msg(map[string]interface{}{"v": 9.0}), nil); len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("all mode: else must not shadow a match: %v", ports)
	}
	if ports := all.SelectPorts(msg(map[string]interface{}{"v": 1.0}), nil); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("all mode: else should catch unmatched: %v", ports)
	}
}

// TestTranslateNRRulesBtwn checks a Node-RED "btwn" rule maps v2/vt2
// onto the native Value2/Value2Type pair.
func TestTranslateNRRulesBtwn(t *testing.T) {
	n := nodestore.NewNode("n1", "switch")
	n.SetData(map[string]interface{}{
		"property":     "payload",
		"propertyType": "msg",
		"rules": []interface{}{
			map[string]interface{}{"t": "btwn", "v": "1", "vt": "num", "v2": "9", "vt2": "num"},
			map[string]interface{}{"t": "else"},
		},
	})
	c := &SwitchConfig{}
	if err := translateNRRules(n, c); err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	if !c.Otherwise {
		t.Fatal("else rule should set the otherwise port")
	}
	if len(c.Rules) != 1 {
		t.Fatalf("expected 1 translated rule, got %d", len(c.Rules))
	}
	r := c.Rules[0]
	if r.Op != "btwn" || r.Value != "1" || r.ValueType != "num" || r.Value2 != "9" || r.Value2Type != "num" {
		t.Fatalf("translated btwn rule wrong: %+v", r)
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
