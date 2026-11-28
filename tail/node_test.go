package tail

// Pack-local unit tests for the tail node — line/binary emit modes,
// pending-line handling and validation, against a fake host and a
// temp file (no engine).

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

// fakeHost captures emitted payloads.
type fakeHost struct {
	mu      sync.Mutex // guards emitted — the poll loop emits from a goroutine
	emitted []nodestore.MessageInterface
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
	return nil
}

func (h *fakeHost) emittedMsgs() []nodestore.MessageInterface {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nodestore.MessageInterface(nil), h.emitted...)
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {}
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

func newNode(cfg *TailConfig, host *fakeHost) *TailNode {
	return NewTailNode(nodestore.NewNode("t1", "tail").SetWorkflowID("wf"), cfg, host)
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

func TestTailFollowsAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.txt")
	_ = os.WriteFile(path, []byte("old\n"), 0644)
	host := &fakeHost{}
	n := newNode(&TailConfig{Filename: path, Interval: 0.02}, host)
	defer func() { _ = n.Stop() }()

	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["status"] != "tailing" {
		t.Fatalf("status wrong: %v", out.GetPayload())
	}
	// append new lines — the node starts at EOF so "old" is not emitted
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	_, _ = f.WriteString("new-line\npartial")
	_ = f.Close()
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "line emission")
	emitted := host.emittedMsgs()
	if emitted[0].GetPayload() != "new-line" {
		t.Fatalf("emitted line wrong: %v", emitted[0].GetPayload())
	}
	if emitted[0].GetMetadata()["filename"] != path {
		t.Fatal("filename metadata missing")
	}
}

func TestTailFromStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.txt")
	_ = os.WriteFile(path, []byte("first\nsecond\n"), 0644)
	host := &fakeHost{}
	n := newNode(&TailConfig{Filename: path, Interval: 0.02, FromStart: true}, host)
	defer func() { _ = n.Stop() }()
	_, _ = n.Execute(context.Background(), nodestore.NewMessage())
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 2 }, "backlog emission")
	if m := host.emittedMsgs()[0]; m.GetPayload() != "first" {
		t.Fatalf("fromStart should replay existing content: %v", m.GetPayload())
	}
}

func TestTailBinary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bin.dat")
	host := &fakeHost{}
	n := newNode(&TailConfig{Filename: path, Interval: 0.02, Filetype: "binary"}, host)
	defer func() { _ = n.Stop() }()
	_, _ = n.Execute(context.Background(), nodestore.NewMessage())
	_ = os.WriteFile(path, []byte{1, 2, 3}, 0644)
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "binary emission")
	if b, ok := host.emittedMsgs()[0].GetPayload().([]byte); !ok || len(b) != 3 {
		t.Fatalf("binary chunk wrong: %v", host.emittedMsgs()[0].GetPayload())
	}
}

func TestTailCustomSeparator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.txt")
	host := &fakeHost{}
	n := newNode(&TailConfig{Filename: path, Interval: 0.02, Split: `\r\n`}, host)
	defer func() { _ = n.Stop() }()
	_, _ = n.Execute(context.Background(), nodestore.NewMessage())
	_ = os.WriteFile(path, []byte("a\r\nb\r\n"), 0644)
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 2 }, "crlf emission")
	emitted := host.emittedMsgs()
	if emitted[0].GetPayload() != "a" || emitted[1].GetPayload() != "b" {
		t.Fatalf("custom separator wrong: %v %v", emitted[0].GetPayload(), emitted[1].GetPayload())
	}
}

func TestTailValidate(t *testing.T) {
	host := &fakeHost{}
	if err := newNode(&TailConfig{Filename: "/tmp/x"}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&TailConfig{}, host).Validate(); err == nil {
		t.Fatal("missing filename should fail")
	}
	if err := newNode(&TailConfig{Filename: "/x", Filetype: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad filetype should fail")
	}
	if err := newNode(&TailConfig{Filename: "/x", Interval: -1}, host).Validate(); err == nil {
		t.Fatal("negative interval should fail")
	}
}
