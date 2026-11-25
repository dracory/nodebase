package html

// Internal coverage ported from flowrunner's node_parity_mopup_test.go
// (the matches field is unexported).

import (
	"context"
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
	if s, _ := node.matches[0].(string); s != "one" {
		t.Fatalf("html mode should return inner HTML, got %v", node.matches[0])
	}
	node, _ = htmlNode(t, &HTMLConfig{Tag: "a", As: "single", Ret: "href"}, testHTMLDoc)
	if node.matches[0] != "/x" {
		t.Fatalf("attr mode: got %v", node.matches[0])
	}
}

// TestHTMLMultiExpand verifies multi mode emits one parts-tagged
// message per match and no matches emit nothing.
func TestHTMLMultiExpand(t *testing.T) {
	node, out := htmlNode(t, &HTMLConfig{Tag: "li", As: "multi", Ret: "text"}, testHTMLDoc)
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

	node, out = htmlNode(t, &HTMLConfig{Tag: "h1", As: "multi"}, testHTMLDoc)
	if got := node.Expand(out); got != nil {
		t.Fatalf("no-match selector must emit nothing, got %v", got)
	}
}

// TestHTMLSingleArray verifies single mode (the Node-RED default)
// returns one message whose payload is an array of matches.
func TestHTMLSingleArray(t *testing.T) {
	_, out := htmlNode(t, &HTMLConfig{Tag: "li", As: "single", Ret: "text"}, testHTMLDoc)
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("expected array of 2, got %v", out.GetPayload())
	}
	if arr[0] != "one" || arr[1] != "two" {
		t.Fatalf("unexpected array %v", arr)
	}
}

// TestHTMLSelectorFromMessage verifies an empty configured tag falls
// back to msg.select.
func TestHTMLSelectorFromMessage(t *testing.T) {
	node := NewHTMLNode(nodestore.NewNode("h", "html"), &HTMLConfig{As: "single", Ret: "text"})
	msg := nodestore.NewMessage().SetPayload(testHTMLDoc)
	msg.SetMetadata(map[string]interface{}{"select": ".hot"})
	out, err := node.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("html Execute failed: %v", err)
	}
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 1 || arr[0] != "two" {
		t.Fatalf("msg.select extraction failed, got %v", out.GetPayload())
	}
}

// TestHTMLOutProperty verifies extraction lands on outproperty.
func TestHTMLOutProperty(t *testing.T) {
	node := NewHTMLNode(nodestore.NewNode("h", "html"),
		&HTMLConfig{Tag: "li", As: "single", Ret: "text", OutProperty: "items"})
	out, err := node.Execute(context.Background(), nodestore.NewMessage().SetPayload(testHTMLDoc))
	if err != nil {
		t.Fatalf("html Execute failed: %v", err)
	}
	arr, ok := out.GetMetadata()["items"].([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("expected items metadata array of 2, got %v", out.GetMetadata())
	}
}
