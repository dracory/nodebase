package filein

// Pack-local unit tests for the fileIn node — all read formats,
// lines/stream Expand fan-out, sendError and validation, using a temp
// dir and a nil host.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *FileInConfig) *FileInNode {
	return NewFileInNode(nodestore.NewNode("f1", "fileIn"), cfg, nil)
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "in.txt")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	return path
}

func TestFileInUTF8(t *testing.T) {
	path := writeTemp(t, "hello")
	n := newNode(&FileInConfig{Filename: path})
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "hello" {
		t.Fatalf("payload wrong: %v", out.GetPayload())
	}
	if out.GetMetadata()["filename"] != path {
		t.Fatal("filename metadata missing")
	}
	// utf8 format passes through unexpanded
	if parts := n.Expand(out); len(parts) != 1 || parts[0] != out {
		t.Fatal("utf8 should not expand")
	}
}

func TestFileInBinary(t *testing.T) {
	path := writeTemp(t, "\x01\x02")
	n := newNode(&FileInConfig{Filename: path, Format: "binary"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	b, ok := out.GetPayload().([]byte)
	if !ok || len(b) != 2 {
		t.Fatalf("binary payload wrong: %v", out.GetPayload())
	}
}

func TestFileInLines(t *testing.T) {
	path := writeTemp(t, "a\nb\r\nc\n")
	n := newNode(&FileInConfig{Filename: path, Format: "lines"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	parts := n.Expand(out)
	// Node-RED emits the trailing empty line a final newline produces
	if len(parts) != 4 {
		t.Fatalf("expected 4 lines, got %d", len(parts))
	}
	if parts[0].GetPayload() != "a" || parts[1].GetPayload() != "b" || parts[2].GetPayload() != "c" || parts[3].GetPayload() != "" {
		t.Fatalf("lines wrong: %v", parts[0].GetPayload())
	}
	pm := parts[0].GetMetadata()["parts"].(map[string]interface{})
	if pm["type"] != "string" || pm["count"] != 4 || pm["ch"] != "\n" {
		t.Fatalf("parts metadata wrong: %v", pm)
	}
}

func TestFileInStream(t *testing.T) {
	path := writeTemp(t, "data")
	n := newNode(&FileInConfig{Filename: path, Format: "stream"})
	out, _ := n.Execute(context.Background(), nodestore.NewMessage())
	parts := n.Expand(out)
	if len(parts) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(parts))
	}
	if string(parts[0].GetPayload().([]byte)) != "data" {
		t.Fatalf("chunk wrong: %v", parts[0].GetPayload())
	}
	pm := parts[0].GetMetadata()["parts"].(map[string]interface{})
	if pm["type"] != "stream" {
		t.Fatalf("parts type wrong: %v", pm)
	}
}

func TestFileInEmptyFile(t *testing.T) {
	path := writeTemp(t, "")
	n := newNode(&FileInConfig{Filename: path, Format: "lines"})
	out, _ := n.Execute(context.Background(), nodestore.NewMessage())
	if parts := n.Expand(out); parts != nil {
		t.Fatal("empty file should emit nothing")
	}
}

func TestFileInMissing(t *testing.T) {
	n := newNode(&FileInConfig{Filename: filepath.Join(t.TempDir(), "nope.txt")})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("missing file should error")
	}
	// sendError converts the failure into an error payload
	n2 := newNode(&FileInConfig{Filename: filepath.Join(t.TempDir(), "nope.txt"), SendError: true})
	out, err := n2.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("sendError should not raise: %v", err)
	}
	m := out.GetPayload().(map[string]interface{})
	if m["error"] == nil {
		t.Fatalf("error payload wrong: %v", m)
	}
}

func TestFileInMsgFilename(t *testing.T) {
	path := writeTemp(t, "x")
	n := newNode(&FileInConfig{FilenameType: "msg"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"filename": path})
	out, err := n.Execute(context.Background(), msg)
	if err != nil || out.GetPayload() != "x" {
		t.Fatalf("msg filename should resolve: %v", err)
	}
}

func TestFileInValidate(t *testing.T) {
	if err := newNode(&FileInConfig{Format: "lines"}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&FileInConfig{Format: "bogus"}).Validate(); err == nil {
		t.Fatal("bad format should fail")
	}
}
