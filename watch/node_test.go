package watch

// Pack-local unit tests for the watch node — snapshot diffing
// (added/changed/removed), recursion and validation, against a fake
// host and a temp dir.

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

func newNode(cfg *WatchConfig, host *fakeHost) *WatchNode {
	return NewWatchNode(nodestore.NewNode("w1", "watch").SetWorkflowID("wf"), cfg, host)
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

func TestWatchEmitsChanges(t *testing.T) {
	dir := t.TempDir()
	host := &fakeHost{}
	n := newNode(&WatchConfig{Filename: dir, Interval: 0.02}, host)
	defer n.Stop()

	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["status"] != "watching" {
		t.Fatalf("status wrong: %v", out.GetPayload())
	}

	// added
	p := filepath.Join(dir, "new.txt")
	os.WriteFile(p, []byte("x"), 0644)
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "added event")
	m := host.emittedMsgs()[0]
	if m.GetMetadata()["event"] != "added" || m.GetMetadata()["type"] != "file" {
		t.Fatalf("added event wrong: %v", m.GetMetadata())
	}
	if m.GetPayload() != p {
		t.Fatalf("payload should be the path: %v", m.GetPayload())
	}

	// removed
	os.Remove(p)
	waitFor(t, func() bool {
		for _, e := range host.emittedMsgs() {
			if e.GetMetadata()["event"] == "removed" {
				return true
			}
		}
		return false
	}, "removed event")
}

func TestWatchChangedAndRecursive(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	os.MkdirAll(sub, 0755)
	p := filepath.Join(sub, "deep.txt")
	os.WriteFile(p, []byte("v1"), 0644)

	host := &fakeHost{}
	n := newNode(&WatchConfig{Filename: dir, Interval: 0.02, Recursive: true}, host)
	defer n.Stop()
	n.Execute(context.Background(), nodestore.NewMessage())

	// modify the deep file: sleep ensures mtime differs on coarse fs timestamps
	time.Sleep(30 * time.Millisecond)
	os.WriteFile(p, []byte("v2-longer"), 0644)
	waitFor(t, func() bool {
		for _, e := range host.emittedMsgs() {
			if e.GetMetadata()["event"] == "changed" {
				return true
			}
		}
		return false
	}, "changed event")
}

func TestWatchValidate(t *testing.T) {
	host := &fakeHost{}
	if err := newNode(&WatchConfig{Filename: "/tmp"}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&WatchConfig{}, host).Validate(); err == nil {
		t.Fatal("missing filename should fail")
	}
	if err := newNode(&WatchConfig{Filename: "/x", Interval: -1}, host).Validate(); err == nil {
		t.Fatal("negative interval should fail")
	}
}
