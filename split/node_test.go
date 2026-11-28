package split

// Pack-local unit tests for the split node — all spltType modes,
// object/array payloads and the helper functions.

import (
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *SplitConfig) *SplitNode {
	return NewSplitNode(nodestore.NewNode("s1", "split"), cfg)
}

func expand(t *testing.T, n *SplitNode, payload interface{}) []nodestore.MessageInterface {
	t.Helper()
	msg := nodestore.NewMessage().SetPayload(payload)
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	return n.Expand(out)
}

func TestSplitString(t *testing.T) {
	n := newNode(&SplitConfig{Splt: ","})
	parts := expand(t, n, "a,b,c")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	if parts[0].GetPayload() != "a" || parts[2].GetPayload() != "c" {
		t.Fatalf("wrong chunks: %v", parts[0].GetPayload())
	}
	pm := parts[0].GetMetadata()["parts"].(map[string]interface{})
	if pm["index"] != 0 || pm["count"] != 3 || pm["type"] != "string" || pm["ch"] != "," {
		t.Fatalf("parts metadata wrong: %v", pm)
	}
}

func TestSplitStringByLen(t *testing.T) {
	n := newNode(&SplitConfig{SpltType: "len", Splt: "2"})
	parts := expand(t, n, "abcde")
	if len(parts) != 3 || parts[0].GetPayload() != "ab" || parts[2].GetPayload() != "e" {
		t.Fatalf("len split wrong: %v", parts)
	}
}

func TestSplitDefaultNewline(t *testing.T) {
	n := newNode(&SplitConfig{})
	parts := expand(t, n, "x\ny")
	if len(parts) != 2 {
		t.Fatalf("default newline split wrong: %d", len(parts))
	}
}

func TestSplitEscapedDelim(t *testing.T) {
	n := newNode(&SplitConfig{Splt: `\n`})
	parts := expand(t, n, "x\ny")
	if len(parts) != 2 {
		t.Fatalf("escaped delim split wrong: %d", len(parts))
	}
}

func TestSplitBuffer(t *testing.T) {
	n := newNode(&SplitConfig{Splt: "|"})
	parts := expand(t, n, []byte("a|b"))
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
	if pm := parts[0].GetMetadata()["parts"].(map[string]interface{}); pm["type"] != "buffer" {
		t.Fatalf("type should be buffer: %v", pm)
	}
	n2 := newNode(&SplitConfig{SpltType: "len", Splt: "2"})
	parts2 := expand(t, n2, []byte("abcd"))
	if len(parts2) != 2 || string(parts2[0].GetPayload().([]byte)) != "ab" {
		t.Fatalf("buffer len split wrong: %v", parts2)
	}
}

func TestSplitArray(t *testing.T) {
	n := newNode(&SplitConfig{})
	parts := expand(t, n, []interface{}{1, 2, 3})
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	// chunk length 1 emits the element itself, not a 1-element array
	if parts[0].GetPayload() != 1 {
		t.Fatalf("array element wrong: %v", parts[0].GetPayload())
	}
	// chunked
	n2 := newNode(&SplitConfig{ArraySplt: 2})
	parts2 := expand(t, n2, []interface{}{1, 2, 3})
	if len(parts2) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(parts2))
	}
	if got := len(parts2[0].GetPayload().([]interface{})); got != 2 {
		t.Fatalf("chunk size wrong: %d", got)
	}
	// typed slice
	n3 := newNode(&SplitConfig{})
	parts3 := expand(t, n3, []string{"a", "b"})
	if len(parts3) != 2 {
		t.Fatalf("typed slice should split: %d", len(parts3))
	}
}

func TestSplitObject(t *testing.T) {
	n := newNode(&SplitConfig{})
	parts := expand(t, n, map[string]interface{}{"b": 2, "a": 1})
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
	// keys sorted
	pm0 := parts[0].GetMetadata()["parts"].(map[string]interface{})
	if pm0["key"] != "a" || parts[0].GetPayload() != 1 {
		t.Fatalf("object split order wrong: %v", pm0)
	}
}

func TestSplitAddname(t *testing.T) {
	n := newNode(&SplitConfig{Addname: "idx"})
	parts := expand(t, n, []interface{}{"x", "y"})
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
	if parts[1].GetMetadata()["idx"] != 1 {
		t.Fatalf("addname should carry the index: %v", parts[1].GetMetadata())
	}
}

func TestSplitNonSplittable(t *testing.T) {
	n := newNode(&SplitConfig{})
	msg := nodestore.NewMessage().SetPayload(42)
	parts := n.Expand(msg)
	if len(parts) != 1 || parts[0] != msg {
		t.Fatal("non-splittable payload should pass through")
	}
	if n.Expand(nil) != nil {
		t.Fatal("nil output should yield nil")
	}
	// empty string → nothing to emit
	if parts := expand(t, n, []interface{}{}); parts != nil {
		t.Fatal("empty array should emit nothing")
	}
}

func TestSplitPropertyPath(t *testing.T) {
	n := newNode(&SplitConfig{Property: "items"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"items": []interface{}{"a", "b"},
	})
	parts := n.Expand(msg)
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
	m := parts[0].GetPayload().(map[string]interface{})
	if m["items"] != "a" {
		t.Fatalf("element should be written at the source path: %v", m)
	}
	pm := parts[0].GetMetadata()["parts"].(map[string]interface{})
	if pm["property"] != "payload.items" {
		t.Fatalf("parts should record the full property path: %v", pm)
	}
}

func TestParseIntLike(t *testing.T) {
	cases := []struct {
		in       string
		fallback int
		want     int
	}{
		{"5", 1, 5},
		{"-3", 1, 1},
		{"3.9", 1, 3},
		{"", 7, 7},
		{"abc", 4, 4},
		{" 12x", 1, 12},
		{"0", 2, 2},
	}
	for _, c := range cases {
		if got := parseIntLike(c.in, c.fallback); got != c.want {
			t.Fatalf("parseIntLike(%q,%d)=%d want %d", c.in, c.fallback, got, c.want)
		}
	}
}
