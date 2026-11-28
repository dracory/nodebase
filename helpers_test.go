package nodebase

// Pack-local unit tests for the shared helpers every node pack builds
// on — the spec suite in noderun exercises them only indirectly through
// engine runs, so the paths it cannot reach (bad configs, edge values,
// metadata paths) are verified here.

import (
	"bytes"
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

// TestDecodeConfig verifies the JSON-map → typed-struct decode every
// factory applies, including the error paths the engine never feeds it.
func TestDecodeConfig(t *testing.T) {
	type cfg struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	node := nodestore.NewNode("n1", "test").SetData(map[string]interface{}{
		"name":  "abc",
		"count": float64(3),
	})
	var c cfg
	if err := DecodeConfig(node, &c); err != nil {
		t.Fatalf("DecodeConfig failed: %v", err)
	}
	if c.Name != "abc" || c.Count != 3 {
		t.Fatalf("decoded incorrectly: %+v", c)
	}

	// Type mismatch → error mentioning the node type
	node2 := nodestore.NewNode("n2", "mytype").SetData(map[string]interface{}{
		"count": "not-a-number",
	})
	var c2 cfg
	if err := DecodeConfig(node2, &c2); err == nil {
		t.Fatal("expected decode error for bad count type")
	}
}

// TestParseJSONOrString covers the value coercion used by change/rbe.
func TestParseJSONOrString(t *testing.T) {
	cases := []struct {
		in   string
		want interface{}
	}{
		{`{"a":1}`, map[string]interface{}{"a": float64(1)}},
		{`[1,2]`, []interface{}{float64(1), float64(2)}},
		{`42`, float64(42)},
		{`"s"`, "s"},
		{`not json`, "not json"},
		{`{bad`, "{bad"},
	}
	for _, c := range cases {
		got := ParseJSONOrString(c.in)
		switch want := c.want.(type) {
		case map[string]interface{}:
			m, ok := got.(map[string]interface{})
			if !ok || m["a"] != want["a"] {
				t.Fatalf("ParseJSONOrString(%q)=%v want %v", c.in, got, c.want)
			}
		case []interface{}:
			arr, ok := got.([]interface{})
			if !ok || len(arr) != len(want) {
				t.Fatalf("ParseJSONOrString(%q)=%v want %v", c.in, got, c.want)
			}
		default:
			if got != c.want {
				t.Fatalf("ParseJSONOrString(%q)=%v want %v", c.in, got, c.want)
			}
		}
	}
}

// TestMsgPath verifies the "msg." prefix handling.
func TestMsgPath(t *testing.T) {
	cases := []struct {
		in   string
		rest string
		ok   bool
	}{
		{"msg.statusCode", "statusCode", true},
		{"msg.a.b", "a.b", true},
		{"msg.", "", false},
		{"payload", "payload", false},
		{"", "", false},
	}
	for _, c := range cases {
		rest, ok := MsgPath(c.in)
		if rest != c.rest || ok != c.ok {
			t.Fatalf("MsgPath(%q)=(%q,%v) want (%q,%v)", c.in, rest, ok, c.rest, c.ok)
		}
	}
}

// TestPathHelpers covers SetPath/GetPath/DeletePath including nested
// creation and non-map intermediates.
func TestPathHelpers(t *testing.T) {
	m := map[string]interface{}{}
	SetPath(m, "a.b.c", 1)
	if v, ok := GetPath(m, "a.b.c"); !ok || v != 1 {
		t.Fatalf("expected a.b.c=1, got %v", m)
	}
	// Overwrite a non-map intermediate
	m["a"] = "scalar"
	SetPath(m, "a.b", 2)
	if v, ok := GetPath(m, "a.b"); !ok || v != 2 {
		t.Fatalf("expected a.b=2 after scalar overwrite, got %v", m)
	}
	// Empty / missing paths
	if _, ok := GetPath(m, ""); ok {
		t.Fatal("empty path should not be found")
	}
	if _, ok := GetPath(map[string]interface{}{"a": "s"}, "a.b"); ok {
		t.Fatal("descent through scalar should fail")
	}
	SetPath(m, "", "x") // no-op
	DeletePath(m, "a.b")
	if _, ok := GetPath(m, "a.b"); ok {
		t.Fatal("a.b should be deleted")
	}
	DeletePath("not-a-map", "x") // no-op, must not panic
	DeletePath(map[string]interface{}{"a": "s"}, "a.b.c")
}

// TestLookupProperty covers payload/metadata resolution order.
func TestLookupProperty(t *testing.T) {
	msg := nodestore.NewMessage().
		SetPayload(map[string]interface{}{"user": map[string]interface{}{"name": "bob"}}).
		SetMetadata(map[string]interface{}{"topic": "t1"})

	if v, ok := LookupProperty(msg, "payload"); !ok {
		t.Fatalf("payload lookup failed: %v", v)
	}
	if v, ok := LookupProperty(msg, "topic"); !ok || v != "t1" {
		t.Fatalf("metadata lookup failed: %v", v)
	}
	if v, ok := LookupProperty(msg, "user.name"); !ok || v != "bob" {
		t.Fatalf("dotted payload lookup failed: %v", v)
	}
	if _, ok := LookupProperty(msg, "missing"); ok {
		t.Fatal("missing path should not be found")
	}
	if _, ok := LookupProperty(nil, "x"); ok {
		t.Fatal("nil message should not resolve")
	}
	if _, ok := LookupProperty(msg, ""); ok {
		t.Fatal("empty path should not resolve")
	}
}

// TestRangeProperty covers the watched-property extraction shared by
// range and rbe.
func TestRangeProperty(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"v": 5})
	if v, ok := RangeProperty(msg, "v"); !ok || v != 5 {
		t.Fatalf("got %v", v)
	}
	scalar := nodestore.NewMessage().SetPayload(7)
	if v, ok := RangeProperty(scalar, "payload"); !ok || v != 7 {
		t.Fatalf("got %v", v)
	}
	if _, ok := RangeProperty(scalar, "v"); ok {
		t.Fatal("path into non-map payload should fail")
	}
	empty := nodestore.NewMessage()
	if _, ok := RangeProperty(empty, "payload"); ok {
		t.Fatal("nil payload should report not found")
	}
}

// TestParseNumeric covers the JS Number() semantics (BOM, radix
// prefixes, NaN handling) the spec suite only touches indirectly.
func TestParseNumeric(t *testing.T) {
	cases := []struct {
		in   interface{}
		want float64
		ok   bool
	}{
		{3.5, 3.5, true},
		{int(4), 4, true},
		{"42", 42, true},
		{"  1.5  ", 1.5, true},
		{"\ufeff7", 7, true},
		{"0x1F", 31, true},
		{"-0x10", -16, true},
		{"+0o17", 15, true},
		{"0b101", 5, true},
		{"-0b11", -3, true},
		{"", 0, false},
		{"abc", 0, false},
		{"0x", 0, false},
		{"NaN", 0, false},
		{nil, 0, true}, // cast.ToFloat64E(nil) coerces to 0
	}
	for _, c := range cases {
		got, ok := ParseNumeric(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Fatalf("ParseNumeric(%v)=(%v,%v) want (%v,%v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

// TestStringHelpers covers StringOf / ParseLinkList / NodeName /
// ParseOverflowPolicy / UnescapeDelim / SortedKeys / NextGroupSeq.
func TestStringHelpers(t *testing.T) {
	if StringOf("x") != "x" || StringOf(3) != "" {
		t.Fatal("StringOf")
	}
	links := ParseLinkList(" a , b ,,c ")
	if len(links) != 3 || links[0] != "a" || links[2] != "c" {
		t.Fatalf("ParseLinkList string: %v", links)
	}
	links = ParseLinkList([]interface{}{"x", "y", 5, ""})
	if len(links) != 2 || links[1] != "y" {
		t.Fatalf("ParseLinkList list: %v", links)
	}
	if ParseLinkList(42) != nil {
		t.Fatal("ParseLinkList non-string should be nil")
	}

	named := nodestore.NewNode("n", "t").SetData(map[string]interface{}{"name": "MyNode"})
	if NodeName(named) != "MyNode" {
		t.Fatal("NodeName should use data.name")
	}
	unnamed := nodestore.NewNode("nid", "t")
	if NodeName(unnamed) != "nid" {
		t.Fatal("NodeName should fall back to id")
	}

	for _, s := range []string{"block", "drop-newest", "drop-oldest", "route-to-catch", " BLOCK "} {
		if _, ok := ParseOverflowPolicy(s); !ok {
			t.Fatalf("ParseOverflowPolicy(%q) should be valid", s)
		}
	}
	if _, ok := ParseOverflowPolicy("bogus"); ok {
		t.Fatal("bogus policy should be invalid")
	}
	if _, ok := ParseOverflowPolicy(5); ok {
		t.Fatal("non-string policy should be invalid")
	}

	if got := UnescapeDelim(`a\nb\tc`); got != "a\nb\tc" {
		t.Fatalf("UnescapeDelim: %q", got)
	}
	if got := UnescapeDelim("plain"); got != "plain" {
		t.Fatalf("UnescapeDelim plain: %q", got)
	}
	if got := UnescapeDelim(`x\qy`); got != `x\qy` {
		t.Fatalf("UnescapeDelim unknown escape: %q", got)
	}

	keys := SortedKeys(map[string]interface{}{"b": 1, "a": 2, "c": 3})
	if keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Fatalf("SortedKeys: %v", keys)
	}

	if a, b := NextGroupSeq(), NextGroupSeq(); b != a+1 {
		t.Fatalf("NextGroupSeq should increment: %d -> %d", a, b)
	}
}

func TestPayloadBytes(t *testing.T) {
	b, err := PayloadBytes("hi")
	if err != nil || string(b) != "hi" {
		t.Fatalf("string conversion wrong: %q, %v", b, err)
	}
	b, err = PayloadBytes([]byte{0, 0xff})
	if err != nil || !bytes.Equal(b, []byte{0, 0xff}) {
		t.Fatalf("binary conversion wrong: %v, %v", b, err)
	}
	b, err = PayloadBytes(map[string]interface{}{"a": 1})
	if err != nil || string(b) != `{"a":1}` {
		t.Fatalf("object conversion wrong: %q, %v", b, err)
	}
	b, err = PayloadBytes(nil)
	if err != nil || string(b) != "null" {
		t.Fatalf("nil conversion wrong: %q, %v", b, err)
	}
	if _, err := PayloadBytes(func() {}); err == nil {
		t.Fatal("unserializable payload should error")
	}
}

// TestClonePayload verifies deep-copy semantics: fresh id, parentMsgId
// recorded, payload/metatada detached, []byte preserved.
func TestClonePayload(t *testing.T) {
	if ClonePayload(nil) != nil {
		t.Fatal("nil clone should be nil")
	}
	src := nodestore.NewMessage().SetID("m1").
		SetPayload(map[string]interface{}{"k": "v"}).
		SetMetadata(map[string]interface{}{"h": "x"})
	clone := ClonePayload(src)
	if clone == nil {
		t.Fatal("clone must not be nil")
	}
	if clone.GetID() == "m1" {
		t.Fatal("clone must get a fresh id")
	}
	if clone.GetMetadata()["parentMsgId"] != "m1" {
		t.Fatal("clone must record parentMsgId")
	}
	// Mutating the clone must not touch the source
	clone.GetPayload().(map[string]interface{})["k"] = "changed"
	if src.GetPayload().(map[string]interface{})["k"] != "v" {
		t.Fatal("payload map shared between clone and source")
	}
	bin := nodestore.NewMessage().SetPayload([]byte{1, 2, 3})
	bc := ClonePayload(bin)
	if bc == nil {
		t.Fatal("binary clone must not be nil")
	}
	b, ok := bc.GetPayload().([]byte)
	if !ok || len(b) != 3 {
		t.Fatalf("binary payload must survive clone: %T", bc.GetPayload())
	}
	b[0] = 9
	if bin.GetPayload().([]byte)[0] != 1 {
		t.Fatal("binary payload shared between clone and source")
	}
	nested := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"blob": []byte{0, 0xff},
		"list": []interface{}{[]byte{3, 4}},
	})
	nc := ClonePayload(nested)
	if nc == nil {
		t.Fatal("nested clone must not be nil")
	}
	nm, ok := nc.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("nested payload clone is %T", nc.GetPayload())
	}
	blob, ok := nm["blob"].([]byte)
	if !ok || !bytes.Equal(blob, []byte{0, 0xff}) {
		t.Fatalf("nested binary payload must survive clone: %#v (%T)", nm["blob"], nm["blob"])
	}
	list, ok := nm["list"].([]interface{})
	if !ok || len(list) != 1 {
		t.Fatalf("nested list clone wrong: %#v", nm["list"])
	}
	item, ok := list[0].([]byte)
	if !ok || !bytes.Equal(item, []byte{3, 4}) {
		t.Fatalf("nested list binary payload must survive clone: %#v (%T)", list[0], list[0])
	}
	item[0] = 9
	if nested.GetPayload().(map[string]interface{})["list"].([]interface{})[0].([]byte)[0] != 3 {
		t.Fatal("nested binary payload shared between clone and source")
	}
	// error carried over
	src.SetError("boom")
	c := ClonePayload(src)
	if c == nil {
		t.Fatal("error clone must not be nil")
	}
	if c.GetError() == nil || *c.GetError() != "boom" {
		t.Fatal("clone should carry the error")
	}
	// non-JSON payload shared rather than dropped
	fn := nodestore.NewMessage().SetPayload(func() {})
	fc := ClonePayload(fn)
	if fc == nil {
		t.Fatal("function clone must not be nil")
	}
	if fc.GetPayload() == nil {
		t.Fatal("non-JSON payload should be shared, not dropped")
	}
}

// TestCloneMetadata verifies the shallow copy.
func TestCloneMetadata(t *testing.T) {
	if m := CloneMetadata(nil); m == nil || len(m) != 0 {
		t.Fatal("nil input should yield empty map")
	}
	src := nodestore.NewMessage().SetMetadata(map[string]interface{}{"a": 1})
	m := CloneMetadata(src)
	m["a"] = 2
	if src.GetMetadata()["a"] != 1 {
		t.Fatal("metadata shared after CloneMetadata")
	}
}

// TestEvalSwitchRule covers every operator the switch/condition nodes
// delegate to, including cases engine tests can't reach (bad regex,
// missing property, istype variants).
func TestEvalSwitchRule(t *testing.T) {
	msg := func(payload interface{}) nodestore.MessageInterface {
		return nodestore.NewMessage().SetPayload(payload)
	}
	cases := []struct {
		name    string
		rule    SwitchRule
		payload interface{}
		want    bool
	}{
		{"eq num", SwitchRule{Property: "v", Op: "eq", Value: "5"}, map[string]interface{}{"v": 5.0}, true},
		{"eq str", SwitchRule{Property: "v", Op: "eq", Value: "abc"}, map[string]interface{}{"v": "abc"}, true},
		{"eq missing", SwitchRule{Property: "v", Op: "eq", Value: "5"}, map[string]interface{}{}, false},
		{"neq hit", SwitchRule{Property: "v", Op: "neq", Value: "5"}, map[string]interface{}{"v": 6.0}, true},
		{"neq missing", SwitchRule{Property: "v", Op: "neq", Value: "5"}, map[string]interface{}{}, true},
		{"gt", SwitchRule{Property: "v", Op: "gt", Value: "5"}, map[string]interface{}{"v": 6.0}, true},
		{"lt", SwitchRule{Property: "v", Op: "lt", Value: "5"}, map[string]interface{}{"v": 4.0}, true},
		{"lte eq", SwitchRule{Property: "v", Op: "lte", Value: "5"}, map[string]interface{}{"v": 5.0}, true},
		{"gte eq", SwitchRule{Property: "v", Op: "gte", Value: "5"}, map[string]interface{}{"v": 5.0}, true},
		{"gt str cmp", SwitchRule{Property: "v", Op: "gt", Value: "b"}, map[string]interface{}{"v": "c"}, true},
		{"contains str", SwitchRule{Property: "v", Op: "contains", Value: "ell"}, map[string]interface{}{"v": "hello"}, true},
		{"contains arr", SwitchRule{Property: "v", Op: "contains", Value: "2"}, map[string]interface{}{"v": []interface{}{1.0, 2.0}}, true},
		{"empty nil", SwitchRule{Property: "v", Op: "empty"}, map[string]interface{}{"v": nil}, true},
		{"empty str", SwitchRule{Property: "v", Op: "empty"}, map[string]interface{}{"v": ""}, true},
		{"empty arr", SwitchRule{Property: "v", Op: "empty"}, map[string]interface{}{"v": []interface{}{}}, true},
		{"empty missing", SwitchRule{Property: "v", Op: "empty"}, map[string]interface{}{}, true},
		{"nempty str", SwitchRule{Property: "v", Op: "nempty"}, map[string]interface{}{"v": "x"}, true},
		{"nempty empty-str", SwitchRule{Property: "v", Op: "nempty"}, map[string]interface{}{"v": ""}, false},
		{"nempty arr", SwitchRule{Property: "v", Op: "nempty"}, map[string]interface{}{"v": []interface{}{1.0}}, true},
		{"nempty empty-arr", SwitchRule{Property: "v", Op: "nempty"}, map[string]interface{}{"v": []interface{}{}}, false},
		{"nempty missing", SwitchRule{Property: "v", Op: "nempty"}, map[string]interface{}{}, false},
		{"regex", SwitchRule{Property: "v", Op: "regex", Value: "^h.llo$"}, map[string]interface{}{"v": "hello"}, true},
		{"regex slashed", SwitchRule{Property: "v", Op: "regex", Value: "/^h.llo$/"}, map[string]interface{}{"v": "hello"}, true},
		{"regex bad", SwitchRule{Property: "v", Op: "regex", Value: "([unclosed"}, map[string]interface{}{"v": "x"}, false},
		{"istype string", SwitchRule{Property: "v", Op: "istype", Value: "string"}, map[string]interface{}{"v": "s"}, true},
		{"istype number", SwitchRule{Property: "v", Op: "istype", Value: "number"}, map[string]interface{}{"v": 1.5}, true},
		{"istype bool", SwitchRule{Property: "v", Op: "istype", Value: "boolean"}, map[string]interface{}{"v": true}, true},
		{"istype array", SwitchRule{Property: "v", Op: "istype", Value: "array"}, map[string]interface{}{"v": []interface{}{}}, true},
		{"istype object", SwitchRule{Property: "v", Op: "istype", Value: "object"}, map[string]interface{}{"v": map[string]interface{}{}}, true},
		{"istype null", SwitchRule{Property: "v", Op: "istype", Value: "null"}, map[string]interface{}{"v": nil}, true},
		{"istype missing-null", SwitchRule{Property: "v", Op: "istype", Value: "null"}, map[string]interface{}{}, true},
		{"istype missing-undef", SwitchRule{Property: "v", Op: "istype", Value: "undefined"}, map[string]interface{}{}, true},
		{"istype bogus", SwitchRule{Property: "v", Op: "istype", Value: "weird"}, map[string]interface{}{"v": 1}, false},
		{"unknown op", SwitchRule{Property: "v", Op: "wibble", Value: "1"}, map[string]interface{}{"v": 1}, false},
		// ValueType (Node-RED vt): the comparison value is coerced
		// before the operator runs.
		{"vt num", SwitchRule{Property: "v", Op: "eq", Value: "5", ValueType: "num"}, map[string]interface{}{"v": 5.0}, true},
		{"vt num str-side", SwitchRule{Property: "v", Op: "eq", Value: "5", ValueType: "num"}, map[string]interface{}{"v": "5"}, true},
		{"vt bool", SwitchRule{Property: "v", Op: "eq", Value: "true", ValueType: "bool"}, map[string]interface{}{"v": true}, true},
		{"vt bool neq", SwitchRule{Property: "v", Op: "neq", Value: "false", ValueType: "bool"}, map[string]interface{}{"v": "false"}, true},
		{"vt json", SwitchRule{Property: "v", Op: "eq", Value: "[1,2]", ValueType: "json"}, map[string]interface{}{"v": []interface{}{1.0, 2.0}}, true},
		{"vt msg", SwitchRule{Property: "v", Op: "eq", Value: "w", ValueType: "msg"}, map[string]interface{}{"v": 5.0, "w": 5.0}, true},
		{"vt msg neq", SwitchRule{Property: "v", Op: "eq", Value: "w", ValueType: "msg"}, map[string]interface{}{"v": 5.0, "w": 6.0}, false},
		{"vt msg payload", SwitchRule{Property: "v", Op: "eq", Value: "payload", ValueType: "msg"}, map[string]interface{}{"v": map[string]interface{}{"x": 1.0}}, false},
		// Value-less ops: no comparison value, strict boolean/null
		// tests on the resolved property.
		{"is true", SwitchRule{Property: "v", Op: "true"}, map[string]interface{}{"v": true}, true},
		{"is true nonbool", SwitchRule{Property: "v", Op: "true"}, map[string]interface{}{"v": "true"}, false},
		{"is true num", SwitchRule{Property: "v", Op: "true"}, map[string]interface{}{"v": 1.0}, false},
		{"is true missing", SwitchRule{Property: "v", Op: "true"}, map[string]interface{}{}, false},
		{"is false", SwitchRule{Property: "v", Op: "false"}, map[string]interface{}{"v": false}, true},
		{"is false nonbool", SwitchRule{Property: "v", Op: "false"}, map[string]interface{}{"v": "false"}, false},
		{"is false missing", SwitchRule{Property: "v", Op: "false"}, map[string]interface{}{}, false},
		{"is null", SwitchRule{Property: "v", Op: "null"}, map[string]interface{}{"v": nil}, true},
		{"is null str", SwitchRule{Property: "v", Op: "null"}, map[string]interface{}{"v": "null"}, false},
		{"is null missing", SwitchRule{Property: "v", Op: "null"}, map[string]interface{}{}, false},
		{"is nnull", SwitchRule{Property: "v", Op: "nnull"}, map[string]interface{}{"v": 0.0}, true},
		{"is nnull null", SwitchRule{Property: "v", Op: "nnull"}, map[string]interface{}{"v": nil}, false},
		{"is nnull missing", SwitchRule{Property: "v", Op: "nnull"}, map[string]interface{}{}, true},
		{"is else", SwitchRule{Property: "v", Op: "else"}, map[string]interface{}{}, true},
		// btwn: inclusive range against Value and Value2.
		{"btwn in", SwitchRule{Property: "v", Op: "btwn", Value: "1", Value2: "10"}, map[string]interface{}{"v": 5.0}, true},
		{"btwn lo", SwitchRule{Property: "v", Op: "btwn", Value: "1", Value2: "10"}, map[string]interface{}{"v": 1.0}, true},
		{"btwn hi", SwitchRule{Property: "v", Op: "btwn", Value: "1", Value2: "10"}, map[string]interface{}{"v": 10.0}, true},
		{"btwn out", SwitchRule{Property: "v", Op: "btwn", Value: "1", Value2: "10"}, map[string]interface{}{"v": 11.0}, false},
		{"btwn missing", SwitchRule{Property: "v", Op: "btwn", Value: "1", Value2: "10"}, map[string]interface{}{}, false},
		{"btwn vt2 num", SwitchRule{Property: "v", Op: "btwn", Value: "1", ValueType: "num", Value2: "10", Value2Type: "num"}, map[string]interface{}{"v": 7.0}, true},
		{"btwn str", SwitchRule{Property: "v", Op: "btwn", Value: "b", Value2: "d"}, map[string]interface{}{"v": "c"}, true},
		// hask: object has the named key.
		{"hask hit", SwitchRule{Property: "v", Op: "hask", Value: "k"}, map[string]interface{}{"v": map[string]interface{}{"k": 1.0}}, true},
		{"hask miss", SwitchRule{Property: "v", Op: "hask", Value: "k"}, map[string]interface{}{"v": map[string]interface{}{"x": 1.0}}, false},
		{"hask nonobj", SwitchRule{Property: "v", Op: "hask", Value: "k"}, map[string]interface{}{"v": "k"}, false},
		{"hask missing", SwitchRule{Property: "v", Op: "hask", Value: "k"}, map[string]interface{}{}, false},
	}
	// propertyType "jsonata": the property is a sequence expression
	// evaluated against the whole message (scalar payload resolves as
	// "payload").
	jsonataCases := []struct {
		name    string
		rule    SwitchRule
		payload interface{}
		want    bool
	}{
		{"jsonata even", SwitchRule{Property: "(payload % 2) = 0", PropertyType: "jsonata", Op: "eq", Value: "true", ValueType: "bool"}, 8.0, true},
		{"jsonata odd", SwitchRule{Property: "(payload % 2) = 0", PropertyType: "jsonata", Op: "eq", Value: "false", ValueType: "bool"}, 7.0, true},
		{"jsonata num cmp", SwitchRule{Property: "payload % 2", PropertyType: "jsonata", Op: "eq", Value: "1", ValueType: "num"}, 7.0, true},
		{"jsonata undef", SwitchRule{Property: "missing = 1", PropertyType: "jsonata", Op: "eq", Value: "true", ValueType: "bool"}, 7.0, false},
	}
	for _, c := range jsonataCases {
		if got := EvalSwitchRule(c.rule, msg(c.payload)); got != c.want {
			t.Fatalf("%s: EvalSwitchRule=%v want %v", c.name, got, c.want)
		}
	}
	for _, c := range cases {
		if got := EvalSwitchRule(c.rule, msg(c.payload)); got != c.want {
			t.Fatalf("%s: EvalSwitchRule=%v want %v", c.name, got, c.want)
		}
	}
	// nil payload must not panic
	if EvalSwitchRule(SwitchRule{Property: "v", Op: "eq", Value: "1"}, nil) {
		t.Fatal("nil payload eq should be false")
	}
	// scalar (non-map) payload: nothing found
	if EvalSwitchRule(SwitchRule{Property: "v", Op: "neq", Value: "1"}, msg("scalar")) != true {
		t.Fatal("neq on non-map payload should pass")
	}
	// Sequence ops read msg.parts (index/count) from metadata.
	pmsg := func(payload interface{}, index, count int) nodestore.MessageInterface {
		m := msg(payload)
		m.SetMetadata(map[string]interface{}{
			"parts": map[string]interface{}{"id": "g1", "index": index, "count": count},
		})
		return m
	}
	seqCases := []struct {
		name         string
		rule         SwitchRule
		index, count int
		want         bool
	}{
		{"head first", SwitchRule{Op: "head", Value: "2", ValueType: "num"}, 0, 5, true},
		{"head edge", SwitchRule{Op: "head", Value: "2", ValueType: "num"}, 1, 5, true},
		{"head past", SwitchRule{Op: "head", Value: "2", ValueType: "num"}, 2, 5, false},
		{"head str-value", SwitchRule{Op: "head", Value: "2"}, 1, 5, true},
		{"tail last", SwitchRule{Op: "tail", Value: "2", ValueType: "num"}, 4, 5, true},
		{"tail edge", SwitchRule{Op: "tail", Value: "2", ValueType: "num"}, 3, 5, true},
		{"tail early", SwitchRule{Op: "tail", Value: "2", ValueType: "num"}, 2, 5, false},
		{"index in", SwitchRule{Op: "index", Value: "1", Value2: "3"}, 2, 5, true},
		{"index lo", SwitchRule{Op: "index", Value: "1", Value2: "3"}, 1, 5, true},
		{"index hi", SwitchRule{Op: "index", Value: "1", Value2: "3"}, 3, 5, true},
		{"index out", SwitchRule{Op: "index", Value: "1", Value2: "3"}, 4, 5, false},
		{"index vt2 num", SwitchRule{Op: "index", Value: "1", ValueType: "num", Value2: "3", Value2Type: "num"}, 3, 5, true},
	}
	for _, c := range seqCases {
		if got := EvalSwitchRule(c.rule, pmsg("x", c.index, c.count)); got != c.want {
			t.Fatalf("%s: EvalSwitchRule=%v want %v", c.name, got, c.want)
		}
	}
	// Sequence ops fail on messages that are not part of a sequence.
	for _, op := range []string{"head", "tail", "index"} {
		if EvalSwitchRule(SwitchRule{Op: op, Value: "1", Value2: "3"}, msg("x")) {
			t.Fatalf("%s without parts should be false", op)
		}
	}
}

// TestRegistry covers registration edge cases the engine path never
// hits (duplicates, empty type, upsert, lookup misses).
func TestRegistry(t *testing.T) {
	reg := NodeRegistration{
		Descriptor: NodeDescriptor{Type: "testreg"},
		Factory:    func(n nodestore.NodeInterface, h NodeHost) (NodeExecutor, error) { return NewPassThroughNode(n), nil },
	}
	if err := RegisterNode(reg); err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}
	if err := RegisterNode(reg); err == nil {
		t.Fatal("duplicate registration should error")
	}
	if err := UpsertNode(reg); err != nil {
		t.Fatalf("UpsertNode should replace: %v", err)
	}
	if err := RegisterNode(NodeRegistration{}); err == nil {
		t.Fatal("empty type should error")
	}
	if err := RegisterNodeType("", reg.Factory); err == nil {
		t.Fatal("empty type id should error")
	}
	if err := RegisterNodeType("nilfac", nil); err == nil {
		t.Fatal("nil factory should error")
	}
	if err := RegisterNodeType("testreg2", reg.Factory); err != nil {
		t.Fatalf("RegisterNodeType failed: %v", err)
	}
	if _, ok := RegisteredNode("testreg"); !ok {
		t.Fatal("RegisteredNode miss")
	}
	if _, ok := RegisteredNode("nope"); ok {
		t.Fatal("RegisteredNode should miss unknown type")
	}
	if _, ok := NodeSchemaFor("testreg"); ok {
		t.Fatal("testreg has no schema")
	}
	if _, ok := NodeSchemaFor("nope"); ok {
		t.Fatal("NodeSchemaFor should miss unknown type")
	}
	// descriptor Set defaults to core
	got, _ := RegisteredNode("testreg")
	if got.Descriptor.Set != "core" {
		t.Fatalf("default set should be core, got %q", got.Descriptor.Set)
	}
	found := false
	for _, r := range RegisteredNodes() {
		if r.Descriptor.Type == "testreg" {
			found = true
		}
	}
	if !found {
		t.Fatal("RegisteredNodes should include testreg")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustRegisterNode should panic on duplicate")
		}
	}()
	MustRegisterNode(reg)
}

// TestPassThroughAndBase covers the shared executor defaults.
func TestPassThroughAndBase(t *testing.T) {
	n := NewPassThroughNode(nodestore.NewNode("p1", "junction"))
	in := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), in)
	if err != nil || out != in {
		t.Fatal("passthrough should return input unchanged")
	}
	if n.ID() != "p1" || n.Type() != "junction" {
		t.Fatal("ID/Type should delegate to the entity")
	}
	if err := n.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	bad := NewPassThroughNode(nodestore.NewNode("", ""))
	if err := bad.Validate(); err == nil {
		t.Fatal("empty id/type should fail validation")
	}
}
