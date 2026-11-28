package doctotext

// Pack-local unit tests for the docToText node — format sniffing and
// the Office zip+XML extraction path (docx/odt members synthesized
// in-test).

import (
	"archive/zip"
	"bytes"
	"context"
	"strings"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *DocToTextConfig) *DocToTextNode {
	return NewDocToTextNode(nodestore.NewNode("d1", "docToText").SetWorkflowID("wf"), cfg)
}

// zipBytes builds a zip archive in memory.
func zipBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, content := range files {
		f, err := w.Create(name)
		if err != nil {
			t.Fatalf("zip create failed: %v", err)
		}
		_, _ = f.Write([]byte(content))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zip close failed: %v", err)
	}
	return buf.Bytes()
}

func TestDetectDocFormat(t *testing.T) {
	if detectDocFormat([]byte("%PDF-1.4 x")) != "pdf" {
		t.Fatal("pdf magic not detected")
	}
	docx := zipBytes(t, map[string]string{"word/document.xml": "<w:p/>"})
	if detectDocFormat(docx) != "docx" {
		t.Fatal("docx not detected")
	}
	odt := zipBytes(t, map[string]string{"content.xml": "<p/>"})
	if detectDocFormat(odt) != "odt" {
		t.Fatal("odt not detected")
	}
	pptx := zipBytes(t, map[string]string{"ppt/slides/slide1.xml": "<p/>"})
	if detectDocFormat(pptx) != "pptx" {
		t.Fatal("pptx not detected")
	}
	if detectDocFormat(zipBytes(t, map[string]string{"x": "y"})) != "txt" {
		t.Fatal("unknown zip should fall back to txt")
	}
	if detectDocFormat([]byte("plain text")) != "txt" {
		t.Fatal("plain bytes should be txt")
	}
}

func TestDocToTextPlain(t *testing.T) {
	n := newNode(&DocToTextConfig{Format: "auto"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("hello doc"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "hello doc" {
		t.Fatalf("text wrong: %v", out.GetPayload())
	}
	doc := out.GetMetadata()["doc"].(map[string]interface{})
	if doc["sourceFormat"] != "txt" || doc["mime"] != "text/plain" {
		t.Fatalf("doc metadata wrong: %v", doc)
	}
}

func TestDocToTextDocx(t *testing.T) {
	xml := `<?xml version="1.0"?><w:document xmlns:w="x"><w:body>` +
		`<w:p><w:r><w:t>First line</w:t></w:r></w:p>` +
		`<w:p><w:r><w:t>Second</w:t></w:r></w:p></w:body></w:document>`
	docx := zipBytes(t, map[string]string{"word/document.xml": xml})
	n := newNode(&DocToTextConfig{Format: "auto"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(docx))
	if err != nil {
		t.Fatalf("docx extract failed: %v", err)
	}
	text := out.GetPayload().(string)
	if !strings.Contains(text, "First line") || !strings.Contains(text, "Second") {
		t.Fatalf("docx text missing: %q", text)
	}
	if out.GetMetadata()["doc"].(map[string]interface{})["sourceFormat"] != "docx" {
		t.Fatal("docx metadata wrong")
	}
}

func TestDocToTextErrors(t *testing.T) {
	n := newNode(&DocToTextConfig{})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("")); err == nil {
		t.Fatal("empty payload should fail")
	}
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(func() {})); err == nil {
		t.Fatal("unserializable payload should fail")
	}
}
