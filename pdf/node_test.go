package pdf

// Pack-local unit tests for the pdf node — text and template render
// modes, metadata stamping, payload coercion.

import (
	"bytes"
	"context"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *PDFConfig) *PDFNode {
	return NewPDFNode(nodestore.NewNode("p1", "pdf").SetWorkflowID("wf"), cfg)
}

func TestPDFTextMode(t *testing.T) {
	n := newNode(&PDFConfig{Mode: "text", Filename: "out.pdf"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("line one\nline two"))
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	doc, _ := out.GetPayload().([]byte)
	if !bytes.HasPrefix(doc, []byte("%PDF")) {
		t.Fatal("output is not a PDF")
	}
	meta := out.GetMetadata()
	if meta["filename"] != "out.pdf" {
		t.Fatalf("filename metadata wrong: %v", meta)
	}
	file := meta["file"].(map[string]interface{})
	if file["contentType"] != "application/pdf" || file["size"].(int) != len(doc) {
		t.Fatalf("file metadata wrong: %v", file)
	}
}

func TestPDFTemplateMode(t *testing.T) {
	n := newNode(&PDFConfig{Mode: "template", Title: "Report", Filename: "r.pdf"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{
		"name": "alice", "count": 3,
	}))
	if err != nil {
		t.Fatalf("template render failed: %v", err)
	}
	if !bytes.HasPrefix(out.GetPayload().([]byte), []byte("%PDF")) {
		t.Fatal("output is not a PDF")
	}
	// non-map payload falls back to plain text rendering
	out2, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("just text"))
	if err != nil || !bytes.HasPrefix(out2.GetPayload().([]byte), []byte("%PDF")) {
		t.Fatal("scalar in template mode should still render")
	}
}

func TestPayloadText(t *testing.T) {
	if payloadText("s") != "s" || payloadText([]byte("b")) != "b" || payloadText(nil) != "" {
		t.Fatal("payloadText scalars wrong")
	}
	if payloadText(map[string]interface{}{"a": 1}) != "{\n  \"a\": 1\n}" {
		t.Fatalf("payloadText json wrong: %q", payloadText(map[string]interface{}{"a": 1}))
	}
}
