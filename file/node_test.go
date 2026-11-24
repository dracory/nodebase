package file

// Pack-local unit tests for the file (write) node — write modes,
// encodings, directory creation and validation, using a temp dir and
// a nil host (unrestricted FileRoot).

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *FileConfig) *FileNode {
	return NewFileNode(nodestore.NewNode("f1", "file"), cfg, nil)
}

func TestFileWriteOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.txt")
	n := newNode(&FileConfig{Filename: path})
	msg := nodestore.NewMessage().SetPayload("hello")
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetMetadata()["filename"] != path {
		t.Fatalf("filename metadata wrong: %v", out.GetMetadata())
	}
	b, _ := os.ReadFile(path)
	if string(b) != "hello" {
		t.Fatalf("file content wrong: %q", b)
	}
	// overwrite truncates
	n.Execute(context.Background(), nodestore.NewMessage().SetPayload("xy"))
	b, _ = os.ReadFile(path)
	if string(b) != "xy" {
		t.Fatalf("overwrite should truncate: %q", b)
	}
}

func TestFileWriteAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.txt")
	n := newNode(&FileConfig{Filename: path, OverwriteFile: "false", AppendNewline: true})
	n.Execute(context.Background(), nodestore.NewMessage().SetPayload("a"))
	n.Execute(context.Background(), nodestore.NewMessage().SetPayload("b"))
	b, _ := os.ReadFile(path)
	if string(b) != "a\nb\n" {
		t.Fatalf("append wrong: %q", b)
	}
}

func TestFileWriteDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.txt")
	os.WriteFile(path, []byte("old"), 0644)
	n := newNode(&FileConfig{Filename: path, OverwriteFile: "delete"})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("new")); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	b, _ := os.ReadFile(path)
	if string(b) != "new" {
		t.Fatalf("delete mode should write fresh: %q", b)
	}
}

func TestFileCreateDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "out.txt")
	n := newNode(&FileConfig{Filename: path, CreateDir: true})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("createDir should make parents")
	}
}

func TestFileEncodings(t *testing.T) {
	dir := t.TempDir()
	// base64: string payload is decoded before writing
	n := newNode(&FileConfig{Filename: filepath.Join(dir, "b64.bin"), Encoding: "base64"})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("aGk=")); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "b64.bin")); string(b) != "hi" {
		t.Fatalf("base64 decode wrong: %q", b)
	}
	// hex
	n2 := newNode(&FileConfig{Filename: filepath.Join(dir, "hex.bin"), Encoding: "hex"})
	n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("6869"))
	if b, _ := os.ReadFile(filepath.Join(dir, "hex.bin")); string(b) != "hi" {
		t.Fatalf("hex decode wrong: %q", b)
	}
	// bad base64 errors
	n3 := newNode(&FileConfig{Filename: filepath.Join(dir, "bad.bin"), Encoding: "base64"})
	if _, err := n3.Execute(context.Background(), nodestore.NewMessage().SetPayload("!!!")); err == nil {
		t.Fatal("bad base64 should error")
	}
	// non-string payload is JSON-marshalled
	n4 := newNode(&FileConfig{Filename: filepath.Join(dir, "j.txt")})
	n4.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{"a": 1}))
	if b, _ := os.ReadFile(filepath.Join(dir, "j.txt")); string(b) != `{"a":1}` {
		t.Fatalf("json marshal wrong: %q", b)
	}
}

func TestFileMsgFilename(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "msg.txt")
	n := newNode(&FileConfig{FilenameType: "msg"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"filename": target})
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetMetadata()["filename"] != target {
		t.Fatal("msg filename should be used")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatal("file should exist at msg filename")
	}
	// missing filename property errors
	n2 := newNode(&FileConfig{FilenameType: "msg"})
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("missing msg filename should error")
	}
}

func TestFileValidate(t *testing.T) {
	if err := newNode(&FileConfig{OverwriteFile: "true", Encoding: "utf8"}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&FileConfig{OverwriteFile: "bogus"}).Validate(); err == nil {
		t.Fatal("bad overwriteFile should fail")
	}
	if err := newNode(&FileConfig{Encoding: "bogus"}).Validate(); err == nil {
		t.Fatal("bad encoding should fail")
	}
}
