package html

// Internal coverage ported from flowrunner's node_parity_mopup_test.go
// (the matches field is unexported).

import (
	"context"
	"strings"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

const testHTMLDoc = `<html><body>
<ul class="list"><li id="a" data-k="1">one</li><li class="hot">two</li></ul>
<div class="note"><a href="/x">link</a></div>
<p class="note">para</p>
</body></html>`

func htmlNode(t *testing.T, cfg *HTMLConfig, data string) (*HTMLNode, nodestore.MessageInterface) {
	t.Helper()
	node := NewHTMLNode(nodestore.NewNode("h", "html"), cfg)
	out, err := node.Execute(context.Background(), nodestore.NewMessage().SetPayload(data))
	if err != nil {
		t.Fatalf("html Execute failed: %v", err)
	}
	return node, out
}

// TestHTMLSelectorModes covers the supported selector subset.
func TestHTMLSelectorModes(t *testing.T) {
	cases := []struct {
		sel   string
		count int
	}{
		{"li", 2},
		{".hot", 1},
		{"#a", 1},
		{"li[data-k]", 1},
		{"li[data-k=1]", 1},
		{"li[data-k=2]", 0},
		{".note", 2},
		{"div a", 1},
		{"div > a", 1},
		{"body > a", 0},
		{"h1, p", 1},
		{"ul.list li.hot", 1},
	}
	for _, c := range cases {
		node, _ := htmlNode(t, &HTMLConfig{Tag: c.sel, As: "single", Ret: "text"}, testHTMLDoc)
		if got := len(node.matches); got != c.count {
			t.Errorf("selector %q: %d matches, want %d", c.sel, got, c.count)
		}
	}
}

// TestHTMLRetModes verifies html/text/attr extraction.
func TestHTMLRetModes(t *testing.T) {
	node, _ := htmlNode(t, &HTMLConfig{Tag: "#a", As: "single", Ret: "text"}, testHTMLDoc)
	if node.matches[0] != "one" {
		t.Fatalf("text mode: got %v", node.matches[0])
	}
	node, _ = htmlNode(t, &HTMLConfig{Tag: "#a", As: "single", Ret: "html"}, testHTMLDoc)
	if s, _ := node.matches[0].(string); !strings.Contains(s, `data-k="1"`) {
		t.Fatalf("html mode: got %v", node.matches[0])
	}
	node, _ = htmlNode(t, &HTMLConfig{Tag: "a", As: "single", Ret: "href"}, testHTMLDoc)
	if node.matches[0] != "/x" {
		t.Fatalf("attr mode: got %v", node.matches[0])
	}
}

// TestHTMLSingleExpand verifies single mode emits one parts-tagged
// message per match and no matches emit nothing.
func TestHTMLSingleExpand(t *testing.T) {
	node, out := htmlNode(t, &HTMLConfig{Tag: "li", As: "single", Ret: "text"}, testHTMLDoc)
	outs := node.Expand(out)
	if len(outs) != 2 {
		t.Fatalf("expected 2 expanded messages, got %d", len(outs))
	}
	for i, o := range outs {
		parts, ok := o.GetMetadata()["parts"].(map[string]interface{})
		if !ok {
			t.Fatalf("msg %d missing parts metadata", i)
		}
		if parts["index"] != i || parts["count"] != 2 {
			t.Fatalf("msg %d parts wrong: %v", i, parts)
		}
	}
	if outs[0].GetPayload() != "one" || outs[1].GetPayload() != "two" {
		t.Fatalf("unexpected payloads %v %v", outs[0].GetPayload(), outs[1].GetPayload())
	}

	node, out = htmlNode(t, &HTMLConfig{Tag: "h1", As: "single"}, testHTMLDoc)
	if got := node.Expand(out); got != nil {
		t.Fatalf("no-match selector must emit nothing, got %v", got)
	}
}

// TestHTMLMultiArray verifies multi mode returns one array payload.
func TestHTMLMultiArray(t *testing.T) {
	_, out := htmlNode(t, &HTMLConfig{Tag: "li", As: "multi", Ret: "text"}, testHTMLDoc)
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("expected array of 2, got %v", out.GetPayload())
	}
	if arr[0] != "one" || arr[1] != "two" {
		t.Fatalf("unexpected array %v", arr)
	}
}
