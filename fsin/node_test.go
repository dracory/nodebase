package fsin

// Pack-local unit tests for the fsIn node — local-locator polling,
// format modes, pattern filter, delete-after-emit and validation,
// against a fake host (no engine).

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost captures emitted payloads and events.
type fakeHost struct {
	mu      sync.Mutex // guards emitted/events — the poll loop emits from a goroutine
	emitted []nodestore.MessageInterface
	events  []string
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, level+":"+message)
}

func (h *fakeHost) emittedMsgs() []nodestore.MessageInterface {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nodestore.MessageInterface(nil), h.emitted...)
}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error { return nil }
func (h *fakeHost) DiscardPending(id string)                                      {}
func (h *fakeHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) {
	return nil, false
}
func (h *fakeHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) { return nil, false }
func (h *fakeHost) RunContext(wf string) context.Context                      { return context.Background() }
func (h *fakeHost) GetOrCreateNodeState(wf, nodeID string, f func() interface{}) interface{} {
	return f()
}
func (h *fakeHost) ExecuteDownstream(ctx context.Context, wf, from string, w nodestore.WorkflowInterface, p nodestore.MessageInterface, ports []string) {
}
func (h *fakeHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) { return nil, false }
func (h *fakeHost) ResolvedContextStore() nodebase.ContextStore         { return nil }
func (h *fakeHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)   { return nil, nil }
func (h *fakeHost) FileRootPath() string                                { return "" }
func (h *fakeHost) ExecAllowed() bool                                   { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

func newNode(cfg *FsInConfig, host *fakeHost) *FsInNode {
	return NewFsInNode(nodestore.NewNode("f1", "fsIn").SetWorkflowID("wf"), cfg, host)
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(15 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestFsInPoll(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("data-a"), 0644)
	host := &fakeHost{}
	n := newNode(&FsInConfig{Location: dir, Interval: 0.02}, host)
	defer n.Stop()

	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["status"] != "watching" {
		t.Fatalf("status payload wrong: %v", out.GetPayload())
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "file emission")
	m := host.emittedMsgs()[0]
	if m.GetPayload() != "data-a" {
		t.Fatalf("payload wrong: %v", m.GetPayload())
	}
	if m.GetMetadata()["filename"] == nil || m.GetMetadata()["filesize"] != int64(6) {
		t.Fatalf("metadata wrong: %v", m.GetMetadata())
	}
}

func TestFsInPatternAndBinary(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "keep.xml"), []byte("\x01"), 0644)
	os.WriteFile(filepath.Join(dir, "skip.txt"), []byte("x"), 0644)
	host := &fakeHost{}
	n := newNode(&FsInConfig{Location: dir, Interval: 0.02, Pattern: "*.xml", Format: "binary"}, host)
	defer n.Stop()
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "xml emission")
	time.Sleep(60 * time.Millisecond) // let a second poll run
	emitted := host.emittedMsgs()
	if len(emitted) != 1 {
		t.Fatalf("pattern should filter to one file, got %d", len(emitted))
	}
	if _, ok := emitted[0].GetPayload().([]byte); !ok {
		t.Fatal("binary format should emit []byte")
	}
}

func TestFsInPathFormat(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f.txt")
	os.WriteFile(p, []byte("x"), 0644)
	host := &fakeHost{}
	n := newNode(&FsInConfig{Location: dir, Interval: 0.02, Format: "path"}, host)
	defer n.Stop()
	n.Execute(context.Background(), nodestore.NewMessage())
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "path emission")
	emitted := host.emittedMsgs()
	if emitted[0].GetPayload() != filepath.Join(dir, "f.txt") {
		t.Fatalf("path format wrong: %v", emitted[0].GetPayload())
	}
}

func TestFsInDeleteAfterEmit(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "gone.txt")
	os.WriteFile(p, []byte("x"), 0644)
	host := &fakeHost{}
	n := newNode(&FsInConfig{Location: dir, Interval: 0.02, Delete: true}, host)
	defer n.Stop()
	n.Execute(context.Background(), nodestore.NewMessage())
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "emission")
	waitFor(t, func() bool {
		_, err := os.Stat(p)
		return os.IsNotExist(err)
	}, "file deletion")
}

func TestFsInBadLocatorAndStop(t *testing.T) {
	host := &fakeHost{}
	n := newNode(&FsInConfig{Location: "s3://x"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("unsupported scheme should error")
	}
	// Stop is idempotent on a never-started node
	if err := n.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestFsInValidate(t *testing.T) {
	host := &fakeHost{}
	if err := newNode(&FsInConfig{Location: "/tmp"}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&FsInConfig{Location: " "}, host).Validate(); err == nil {
		t.Fatal("empty location should fail")
	}
	if err := newNode(&FsInConfig{Location: "ftp://u:pw@h/x"}, host).Validate(); err == nil {
		t.Fatal("password in URI should fail validation")
	}
	if err := newNode(&FsInConfig{Location: "/tmp", Format: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad format should fail")
	}
}

func TestFsInEmitAfterStop(t *testing.T) {
	// A poll in flight when Stop() runs used to nil-deref the closed
	// fs handle — emitFile must bail instead.
	host := &fakeHost{}
	n := newNode(&FsInConfig{Location: t.TempDir()}, host)
	if err := n.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	n.emitFile("/x", nodebase.FsEntry{Name: "x"})
}
