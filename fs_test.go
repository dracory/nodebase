package nodebase

// Pack-local unit tests for the Fs seam: locator parsing, credential
// extraction, the LocalFs adapter and the redial decorator — the paths
// the noderun fs spec cannot reach without real sftp/ftp servers.

import (
	"bytes"
	"path/filepath"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

func TestParseFsLocator(t *testing.T) {
	cases := []struct {
		in         string
		wantScheme string
		wantErr    bool
	}{
		{"/tmp/dir", "file", false},
		{"relative/dir", "file", false},
		{"file:///tmp/dir", "file", false},
		{"sftp://user@host/path", "sftp", false},
		{"ftp://user@host/", "ftp", false},
		{"s3://bucket/x", "", true},
		{"gopher://h/x", "", true},
		{"", "", true},
		{"  ", "", true},
	}
	for _, c := range cases {
		scheme, dir, _, err := ParseFsLocator(c.in)
		if c.wantErr {
			if err == nil {
				t.Fatalf("ParseFsLocator(%q) should error", c.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseFsLocator(%q) errored: %v", c.in, err)
		}
		if scheme != c.wantScheme {
			t.Fatalf("ParseFsLocator(%q) scheme=%q want %q", c.in, scheme, c.wantScheme)
		}
		if dir == "" && c.in != "" {
			t.Fatalf("ParseFsLocator(%q) empty dir", c.in)
		}
	}
	// sftp empty path defaults to "/"
	_, dir, _, err := ParseFsLocator("sftp://u@h")
	if err != nil || dir != "/" {
		t.Fatalf("empty sftp path should default to /: %q", dir)
	}
}

func TestOpenFsLocal(t *testing.T) {
	fsys, dir, err := OpenFs(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenFs failed: %v", err)
	}
	if _, ok := fsys.(LocalFs); !ok {
		t.Fatalf("local locator should yield LocalFs, got %T", fsys)
	}
	if dir == "" {
		t.Fatal("dir missing")
	}
	if err := fsys.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestLocalFs(t *testing.T) {
	dir := t.TempDir()
	fs := LocalFs{}
	p := fs.Join(dir, "a.txt")
	if err := fs.WriteFile(p, []byte("x")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := fs.AppendFile(p, []byte("y")); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	b, err := fs.ReadFile(p)
	if err != nil || string(b) != "xy" {
		t.Fatalf("read wrong: %q", b)
	}
	entries, err := fs.List(dir)
	if err != nil || len(entries) != 1 || entries[0].Name != "a.txt" || entries[0].Size != 2 {
		t.Fatalf("list wrong: %v", entries)
	}
	p2 := fs.Join(dir, "b.txt")
	if err := fs.Rename(p, p2); err != nil {
		t.Fatalf("rename failed: %v", err)
	}
	if err := fs.MkdirAll(filepath.Join(dir, "deep", "deeper")); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := fs.Remove(p2); err != nil {
		t.Fatalf("remove failed: %v", err)
	}
	// removing a missing file is not an error
	if err := fs.Remove(filepath.Join(dir, "nope")); err != nil {
		t.Fatalf("remove-missing should be nil: %v", err)
	}
}

func TestFsCredentialsFromData(t *testing.T) {
	creds := FsCredentialsFromData(map[string]interface{}{
		"password":         "secret",
		"keyFile":          "/keys/id",
		"hostKey":          "ssh-ed25519 AAAA",
		"acceptAnyHostKey": true,
	})
	if creds.Password != "secret" || creds.KeyFile != "/keys/id" ||
		creds.HostKey != "ssh-ed25519 AAAA" || !creds.AcceptAnyHostKey {
		t.Fatalf("credentials wrong: %+v", creds)
	}
	// sentinel values are stripped — they are placeholders, not secrets
	creds2 := FsCredentialsFromData(map[string]interface{}{
		"password":         nodestore.CredentialSentinel,
		"acceptAnyHostKey": "true",
	})
	if creds2.Password != "" || !creds2.AcceptAnyHostKey {
		t.Fatalf("sentinel/string coercion wrong: %+v", creds2)
	}
}

func TestResolveFilePath(t *testing.T) {
	// empty name errors
	if _, err := ResolveFilePath("", ""); err == nil {
		t.Fatal("empty name should error")
	}
	// no root → passthrough
	if got, err := ResolveFilePath("", "/tmp/x"); err != nil || got == "" {
		t.Fatalf("unrestricted resolve failed: %v", err)
	}
	// containment
	root := t.TempDir()
	inside := filepath.Join(root, "ok.txt")
	if _, err := ResolveFilePath(root, inside); err != nil {
		t.Fatalf("inside path should resolve: %v", err)
	}
	if _, err := ResolveFilePath(root, filepath.Join(root, "..", "escape.txt")); err == nil {
		t.Fatal(".. escape should be rejected")
	}
}

func TestEncodeFilePayload(t *testing.T) {
	b, err := EncodeFilePayload("hi", "utf8")
	if err != nil || string(b) != "hi" {
		t.Fatalf("utf8 wrong: %q", b)
	}
	b, _ = EncodeFilePayload([]byte{1, 2}, "")
	if len(b) != 2 {
		t.Fatal("[]byte passthrough wrong")
	}
	b, err = EncodeFilePayload([]byte{0, 0xff}, "base64")
	if err != nil || !bytes.Equal(b, []byte{0, 0xff}) {
		t.Fatalf("[]byte must stay binary even with base64 encoding: %v, %v", b, err)
	}
	b, _ = EncodeFilePayload(map[string]interface{}{"a": 1}, "")
	if string(b) != `{"a":1}` {
		t.Fatalf("json marshal wrong: %q", b)
	}
	b, err = EncodeFilePayload("aGk=", "base64")
	if err != nil || string(b) != "hi" {
		t.Fatalf("base64 wrong: %q", b)
	}
	if _, err := EncodeFilePayload("!!!", "base64"); err == nil {
		t.Fatal("bad base64 should error")
	}
	b, err = EncodeFilePayload("6869", "hex")
	if err != nil || string(b) != "hi" {
		t.Fatalf("hex wrong: %q", b)
	}
	if _, err := EncodeFilePayload("zz", "hex"); err == nil {
		t.Fatal("bad hex should error")
	}
}

func TestFileFilename(t *testing.T) {
	// fixed name
	got, err := FileFilename("some/file.txt", "fixed", nil, nil, "")
	if err != nil || got == "" {
		t.Fatalf("fixed filename failed: %v", err)
	}
	// msg filename
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{"filename": "frommsg.txt"})
	got, err = FileFilename("", "msg", msg, nil, "")
	if err != nil || got != "frommsg.txt" {
		t.Fatalf("msg filename wrong: %q", got)
	}
	if _, err := FileFilename("", "msg", nil, nil, ""); err == nil {
		t.Fatal("missing msg filename should error")
	}
}

func TestFileFilenameContext(t *testing.T) {
	// flow/global filename types read the configured value as a
	// context key through the host's context store.
	store := NewMemoryContextStore()
	if err := store.SetContext(ContextScopeFlow, "wf1", "src", "/tmp/flow.txt"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetContext(ContextScopeGlobal, "", "src", "/tmp/global.txt"); err != nil {
		t.Fatal(err)
	}
	h := &stubHost{store: store}
	got, err := FileFilename("src", "flow", nil, h, "wf1")
	if err != nil || got != "/tmp/flow.txt" {
		t.Fatalf("flow filename wrong: %q, %v", got, err)
	}
	got, err = FileFilename("src", "global", nil, h, "wf1")
	if err != nil || got != "/tmp/global.txt" {
		t.Fatalf("global filename wrong: %q, %v", got, err)
	}
	if _, err := FileFilename("missing", "flow", nil, h, "wf1"); err == nil {
		t.Fatal("missing context key should error")
	}
	if _, err := FileFilename("src", "global", nil, nil, "wf1"); err == nil {
		t.Fatal("context filename without a host store should error")
	}
}
