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

// ValueType honors Node-RED's tot: the typedInput value type decides
// how the literal is interpreted (str/num/bool/json/msg).
func TestChangeSetValueTypes(t *testing.T) {
	msg := run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "set", Field: "n", Value: "abc", ValueType: "num"},
		{Op: "set", Field: "f", Value: "1.5", ValueType: "num"},
		{Op: "set", Field: "b", Value: "true", ValueType: "bool"},
		{Op: "set", Field: "arr", Value: "[1,2]", ValueType: "json"},
		{Op: "set", Field: "s", Value: "12", ValueType: "str"},
		{Op: "set", Field: "copy", Value: "n", ValueType: "msg"},
	}}, nodestore.NewMessage())
	m := msg.GetPayload().(map[string]interface{})
	if m["n"] != "abc" {
		t.Fatalf("num non-numeric should stay a string: %v", m["n"])
	}
	if m["f"] != 1.5 {
		t.Fatalf("num should parse to float: %v", m["f"])
	}
	if m["b"] != true {
		t.Fatalf("bool should parse: %v", m["b"])
	}
	if a, ok := m["arr"].([]interface{}); !ok || len(a) != 2 {
		t.Fatalf("json should parse to array: %v", m["arr"])
	}
	// str keeps the literal verbatim — no JSON coercion.
	if m["s"] != "12" {
		t.Fatalf("str should stay a string: %v", m["s"])
	}
	// msg type copies from another message property.
	if m["copy"] != "abc" {
		t.Fatalf("msg copy should read the source path: %v", m["copy"])
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

func TestChangeReplace(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"name": "hello world",
	})
	msg = run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "replace", Field: "name", Pattern: "world", Replacement: "there"},
	}}, msg)
	if got := msg.GetPayload().(map[string]interface{})["name"]; got != "hello there" {
		t.Fatalf("replace wrong: %v", msg.GetPayload())
	}
}

// Capture-group backrefs ($1) in the replacement follow
// regexp.ReplaceAllString semantics.
func TestChangeReplaceBackrefs(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"full": "Doe, John",
	})
	msg = run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "replace", Field: "full", Pattern: `(\w+), (\w+)`, Replacement: "$2 $1"},
	}}, msg)
	if got := msg.GetPayload().(map[string]interface{})["full"]; got != "John Doe" {
		t.Fatalf("backref replace wrong: %v", msg.GetPayload())
	}
}

// Non-string targets behave like set: the value is stringified,
// replaced, and written back as a string.
func TestChangeReplaceNonString(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"n": 12345,
	})
	msg = run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "replace", Field: "n", Pattern: "3", Replacement: "X"},
	}}, msg)
	if got := msg.GetPayload().(map[string]interface{})["n"]; got != "12X45" {
		t.Fatalf("non-string replace wrong: %v", msg.GetPayload())
	}
}

func TestChangeReplaceMetadataAndSkips(t *testing.T) {
	msg := nodestore.NewMessage().
		SetMetadata(map[string]interface{}{"topic": "a.b.c"}).
		SetPayload(map[string]interface{}{"x": "y"})
	msg = run(t, &ChangeConfig{Rules: []ChangeRule{
		{Op: "replace", Field: "msg.topic", Pattern: `\.`, Replacement: "-"},
		{Op: "replace", Field: "missing", Pattern: "z", Replacement: "w"},
		{Op: "replace", Field: "x", Pattern: "[invalid(", Replacement: "w"},
	}}, msg)
	if got := msg.GetMetadata()["topic"]; got != "a-b-c" {
		t.Fatalf("metadata replace wrong: %v", msg.GetMetadata())
	}
	if got := msg.GetPayload().(map[string]interface{})["x"]; got != "y" {
		t.Fatalf("invalid pattern should be skipped: %v", msg.GetPayload())
	}
}

// Node-RED's t:"change" rule shape (from, to, fromt, /pattern/flags)
// translates onto the native replace op via translateNRRules.
func TestTranslateNRChangeRule(t *testing.T) {
	n := nodestore.NewNode("c1", "change")
	n.SetData(map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{
				"t": "change", "p": "payload.name", "pt": "msg",
				"from": "/(foo)+/i", "to": "[$1]", "fromt": "re",
			},
		},
	})
	c := new(ChangeConfig)
	if err := translateNRRules(n, c); err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	if len(c.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %v", c.Rules)
	}
	r := c.Rules[0]
	if r.Op != "replace" || r.Field != "name" || r.Pattern != "(?i)(foo)+" || r.Replacement != "[$1]" {
		t.Fatalf("translated rule wrong: %+v", r)
	}

	// The translated rule executes: /(foo)+/i on "FOOfooX" -> "[$1]"
	// where $1 is the last capture-group iteration.
	msg := run(t, c, nodestore.NewMessage().SetPayload(map[string]interface{}{"name": "FOOfooX"}))
	if got := msg.GetPayload().(map[string]interface{})["name"]; got != "[foo]X" {
		t.Fatalf("executed replace wrong: %v", msg.GetPayload())
	}
}

// nrPattern conversions: /flags wrapper, literal "str" search, and
// rejection of unsupported search types/flags.
func TestNRPattern(t *testing.T) {
	cases := []struct{ from, fromt, want string }{
		{"abc", "", "abc"},
		{"/foo/i", "re", "(?i)foo"},
		{"/a.c/ms", "re", "(?ms)a.c"},
		{"/a/g", "re", "a"},
		{"a.c", "str", `a\.c`},
	}
	for _, c := range cases {
		got, err := nrPattern(c.from, c.fromt)
		if err != nil || got != c.want {
			t.Fatalf("nrPattern(%q,%q)=(%q,%v) want (%q,nil)", c.from, c.fromt, got, err, c.want)
		}
	}
	if _, err := nrPattern("x", "env"); err == nil {
		t.Fatal("fromt env should fail")
	}
	if _, err := nrPattern("/x/u", "re"); err == nil {
		t.Fatal("unsupported flag should fail")
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
