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

// TestTranslateNRRulesJSONataExp checks a Node-RED "jsonata_exp" rule
// decodes into the native op carrying the raw expression, and that it
// routes on the expression's boolean result (=== true per Node-RED).
func TestTranslateNRRulesJSONataExp(t *testing.T) {
	n := nodestore.NewNode("n1", "switch")
	n.SetData(map[string]interface{}{
		"property":     "payload",
		"propertyType": "msg",
		"rules": []interface{}{
			map[string]interface{}{"t": "jsonata_exp", "v": "(payload % 2) = 0", "vt": "jsonata"},
			map[string]interface{}{"t": "jsonata_exp", "v": "(payload % 2) = 1", "vt": "jsonata"},
		},
	})
	c := &SwitchConfig{}
	if err := translateNRRules(n, c); err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	if len(c.Rules) != 2 || c.Rules[0].Op != "jsonata_exp" || c.Rules[0].Value != "(payload % 2) = 0" {
		t.Fatalf("translated rules wrong: %+v", c.Rules)
	}
	node := NewSwitchNode(n, c)
	if ports := node.SelectPorts(msg(8.0), nil); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("even payload should route output_1: %v", ports)
	}
	if ports := node.SelectPorts(msg(7.0), nil); len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("odd payload should route output_2: %v", ports)
	}
	// A non-boolean expression result never matches (=== true).
	c2 := &SwitchConfig{Rules: []nodebase.SwitchRule{
		{Op: "jsonata_exp", Value: `"truthy string"`, ValueType: "jsonata"},
	}}
	if ports := NewSwitchNode(n, c2).SelectPorts(msg(nil), nil); len(ports) != 0 {
		t.Fatalf("non-boolean result must not match: %v", ports)
	}
}

// TestTranslateNRRulesJSONataExpInvalid checks an uncompilable
// jsonata_exp expression still fails loud at deploy.
func TestTranslateNRRulesJSONataExpInvalid(t *testing.T) {
	n := nodestore.NewNode("n1", "switch")
	n.SetData(map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"t": "jsonata_exp", "v": "payload & (", "vt": "jsonata"},
		},
	})
	if err := translateNRRules(n, &SwitchConfig{}); err == nil {
		t.Fatal("invalid JSONata expression should fail translation")
	}
}

// TestSwitchJSONataValueType checks a vt:"jsonata" comparison value is
// evaluated against the message for ordinary ops — the match proves
// the expression ran (a literal "payload" string could never equal 6).
func TestSwitchJSONataValueType(t *testing.T) {
	node := newNode(&SwitchConfig{Rules: []nodebase.SwitchRule{
		{Property: "", Op: "eq", Value: "payload", ValueType: "jsonata"},
	}})
	if ports := node.SelectPorts(msg(6.0), nil); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("payload 6 should equal the evaluated expression: %v", ports)
	}
	node2 := newNode(&SwitchConfig{Rules: []nodebase.SwitchRule{
		{Property: "", Op: "eq", Value: "payload - 1", ValueType: "jsonata"},
	}})
	if ports := node2.SelectPorts(msg(6.0), nil); len(ports) != 0 {
		t.Fatalf("6 should not equal 6-1: %v", ports)
	}
}
