package fsout

// Pack-local unit tests for the fsOut node — local-locator writes via
// tmp+rename, append/delete modes, baseName sandboxing and validation.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *FsOutConfig) *FsOutNode {
	return NewFsOutNode(nodestore.NewNode("f1", "fsOut"), cfg, nil)
}

func TestFsOutWrite(t *testing.T) {
	dir := t.TempDir()
	n := newNode(&FsOutConfig{Location: dir, Filename: "out.txt"})
	msg := nodestore.NewMessage().SetPayload("hello")
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	want := filepath.Join(dir, "out.txt")
	if out.GetMetadata()["filename"] != want {
		t.Fatalf("filename metadata wrong: %v", out.GetMetadata()["filename"])
	}
	b, _ := os.ReadFile(want)
	if string(b) != "hello" {
		t.Fatalf("content wrong: %q", b)
	}
	// no .tmp left behind
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("tmp file leaked: %v", entries)
	}
}

func TestFsOutAppend(t *testing.T) {
	dir := t.TempDir()
	n := newNode(&FsOutConfig{Location: dir, Filename: "a.txt", OverwriteFile: "false", AppendNewline: true})
	n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x"))
	n.Execute(context.Background(), nodestore.NewMessage().SetPayload("y"))
	b, _ := os.ReadFile(filepath.Join(dir, "a.txt"))
	if string(b) != "x\ny\n" {
		t.Fatalf("append wrong: %q", b)
	}
}

func TestFsOutDeleteAndCreateDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested")
	os.MkdirAll(dir, 0755)
	os.WriteFile(filepath.Join(dir, "d.txt"), []byte("old"), 0644)
	n := newNode(&FsOutConfig{Location: dir, Filename: "d.txt", OverwriteFile: "delete"})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("new")); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	// createDir on missing locator
	dir2 := filepath.Join(t.TempDir(), "x", "y")
	n2 := newNode(&FsOutConfig{Location: dir2, Filename: "f.txt", CreateDir: true})
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("z")); err != nil {
		t.Fatalf("createDir execute failed: %v", err)
	}
}

func TestFsOutMsgFilename(t *testing.T) {
	dir := t.TempDir()
	n := newNode(&FsOutConfig{Location: dir, FilenameType: "msg"})
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"filename": "/evil/../sub/out.txt"})
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	// reduced to base name — cannot escape the locator dir
	if out.GetMetadata()["filename"] != filepath.Join(dir, "out.txt") {
		t.Fatalf("msg filename should be base-named: %v", out.GetMetadata()["filename"])
	}
	// missing filename errors
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("missing filename should error")
	}
}

func TestFsOutBadLocator(t *testing.T) {
	n := newNode(&FsOutConfig{Location: "s3://bucket/x", Filename: "f"})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("unsupported scheme should error")
	}
	n2 := newNode(&FsOutConfig{Location: t.TempDir()})
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("empty filename should error")
	}
}

func TestFsOutValidate(t *testing.T) {
	if err := newNode(&FsOutConfig{Location: "/tmp"}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&FsOutConfig{Location: "  "}).Validate(); err == nil {
		t.Fatal("empty location should fail")
	}
	if err := newNode(&FsOutConfig{Location: "sftp://u:pw@h/x"}).Validate(); err == nil {
		t.Fatal("password in URI should fail validation")
	}
	if err := newNode(&FsOutConfig{Location: "/tmp", OverwriteFile: "bogus"}).Validate(); err == nil {
		t.Fatal("bad overwriteFile should fail")
	}
	if err := newNode(&FsOutConfig{Location: "/tmp", Encoding: "bogus"}).Validate(); err == nil {
		t.Fatal("bad encoding should fail")
	}
}
