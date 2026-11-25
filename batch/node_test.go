package batch

// Pack-local unit tests for the batch node — count/concat/interval
// modes, overlap and validation, run against a fake host that captures
// emitted sequence messages.
//
// Node-RED semantics: a completed batch is a *sequence* of the buffered
// messages re-tagged with parts {id, index, count} — emitted via
// ExecuteDownstream — not a single array message.

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost with a state slot and an
// emitted-sequence capture (ExecuteDownstream records each message).
type fakeHost struct {
	states  map[string]interface{}
	mu      sync.Mutex // guards emitted — flushes emit from goroutines
	emitted []nodestore.MessageInterface
}

func newFakeHost() *fakeHost { return &fakeHost{states: map[string]interface{}{}} }

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

// waitFor blocks until n messages were emitted or the deadline passes.
func (h *fakeHost) waitFor(n int, timeout time.Duration) []nodestore.MessageInterface {
	deadline := time.Now().Add(timeout)
	for len(h.emittedMsgs()) < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	return h.emittedMsgs()
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
func (h *fakeHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) {
	return nodestore.NewWorkflow("wf"), true
}
func (h *fakeHost) RunContext(wf string) context.Context { return context.Background() }
func (h *fakeHost) GetOrCreateNodeState(wf, nodeID string, f func() interface{}) interface{} {
	key := wf + "/" + nodeID
	if s, ok := h.states[key]; ok {
		return s
	}
	s := f()
	h.states[key] = s
	return s
}
func (h *fakeHost) ExecuteDownstream(ctx context.Context, wf, from string, w nodestore.WorkflowInterface, p nodestore.MessageInterface, ports []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
}
func (h *fakeHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) { return nil, false }
func (h *fakeHost) ResolvedContextStore() nodebase.ContextStore         { return nil }
func (h *fakeHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)   { return nil, nil }
func (h *fakeHost) FileRootPath() string                                { return "" }
func (h *fakeHost) ExecAllowed() bool                                   { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

func newNode(cfg *BatchConfig) (*BatchNode, *fakeHost) {
	host := newFakeHost()
	return NewBatchNode(nodestore.NewNode("b1", "batch").SetWorkflowID("wf"), cfg, host), host
}

// payloads extracts the emitted payloads in order.
func payloads(msgs []nodestore.MessageInterface) []interface{} {
	out := make([]interface{}, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.GetPayload())
	}
	return out
}

func TestBatchCount(t *testing.T) {
	n, host := newNode(&BatchConfig{Mode: "count", Count: 2})
	ctx := context.Background()
	out, err := n.Execute(ctx, nodestore.NewMessage().SetPayload("a"))
	if err != nil || out != nil {
		t.Fatal("first message should buffer")
	}
	out, err = n.Execute(ctx, nodestore.NewMessage().SetPayload("b"))
	if err != nil || out != nil {
		t.Fatal("completing message is also dropped — the batch emits as a sequence")
	}
	emitted := host.waitFor(2, 2*time.Second)
	if len(emitted) != 2 {
		t.Fatalf("expected a 2-message sequence, got %d", len(emitted))
	}
	got := payloads(emitted)
	if got[0] != "a" || got[1] != "b" {
		t.Fatalf("sequence payloads wrong: %v", got)
	}
	// parts are renumbered on the shared sequence id
	for i, m := range emitted {
		parts := m.GetMetadata()["parts"].(map[string]interface{})
		if parts["index"] != i || parts["count"] != 2 || parts["id"] == "" {
			t.Fatalf("parts wrong at %d: %v", i, parts)
		}
	}
}

func TestBatchCountOverlap(t *testing.T) {
	n, host := newNode(&BatchConfig{Mode: "count", Count: 2, Overlap: 1})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("a"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("b"))
	// overlap keeps "b" → next batch needs only one more
	n.Execute(ctx, nodestore.NewMessage().SetPayload("c"))
	emitted := host.waitFor(4, 2*time.Second)
	if len(emitted) != 4 {
		t.Fatalf("expected two 2-message sequences, got %d", len(emitted))
	}
	got := payloads(emitted)
	if got[0] != "a" || got[1] != "b" || got[2] != "b" || got[3] != "c" {
		t.Fatalf("overlap sequences wrong: %v", got)
	}
}

func TestBatchHonourParts(t *testing.T) {
	n, host := newNode(&BatchConfig{Mode: "count", Count: 3, HonourParts: true})
	ctx := context.Background()
	mk := func(id string, i, count int, v interface{}) nodestore.MessageInterface {
		m := nodestore.NewMessage().SetPayload(v)
		m.SetMetadata(map[string]interface{}{
			"parts": map[string]interface{}{"id": id, "index": i, "count": count},
		})
		return m
	}
	// a completed incoming sequence flushes the batch early
	n.Execute(ctx, mk("g1", 0, 2, "x"))
	n.Execute(ctx, mk("g1", 1, 2, "y"))
	emitted := host.waitFor(2, 2*time.Second)
	if got := payloads(emitted); len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("honourParts should flush on sequence end: %v", got)
	}
}

func TestBatchConcat(t *testing.T) {
	n, host := newNode(&BatchConfig{Mode: "concat", Topics: []struct {
		Topic string `json:"topic"`
	}{{Topic: "t"}}})
	ctx := context.Background()
	mk := func(i int, v interface{}) nodestore.MessageInterface {
		m := nodestore.NewMessage().SetPayload(v)
		m.SetMetadata(map[string]interface{}{
			"topic": "t",
			"parts": map[string]interface{}{"id": "g", "index": i, "count": 3},
		})
		return m
	}
	if out, _ := n.Execute(ctx, mk(0, 1.0)); out != nil {
		t.Fatal("incomplete concat should buffer")
	}
	n.Execute(ctx, mk(1, 2.0))
	n.Execute(ctx, mk(2, "tail"))
	emitted := host.waitFor(3, 2*time.Second)
	got := payloads(emitted)
	// the topic sequence is re-emitted as one renumbered sequence
	if len(got) != 3 || got[0] != 1.0 || got[2] != "tail" {
		t.Fatalf("concat wrong: %v", got)
	}
}

func TestBatchConcatNoParts(t *testing.T) {
	n, _ := newNode(&BatchConfig{Mode: "concat", Topics: []struct {
		Topic string `json:"topic"`
	}{{Topic: "t"}}})
	msg := nodestore.NewMessage().SetPayload("x")
	msg.SetMetadata(map[string]interface{}{"topic": "t"})
	out, err := n.Execute(context.Background(), msg)
	// Node-RED errors "no-parts" — the message is dropped
	if err != nil || out != nil {
		t.Fatal("message without parts should drop in concat mode")
	}
}

func TestBatchInterval(t *testing.T) {
	n, host := newNode(&BatchConfig{Mode: "interval", Interval: 0.02})
	ctx := context.Background()
	out, err := n.Execute(ctx, nodestore.NewMessage().SetPayload("a"))
	if err != nil || out != nil {
		t.Fatal("interval mode should buffer")
	}
	n.Execute(ctx, nodestore.NewMessage().SetPayload("b"))
	emitted := host.waitFor(2, 2*time.Second)
	got := payloads(emitted)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("interval sequence wrong: %v", got)
	}
}

func TestBatchValidate(t *testing.T) {
	n, _ := newNode(&BatchConfig{Mode: "count"})
	if err := n.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad, _ := newNode(&BatchConfig{Mode: "bogus"})
	if err := bad.Validate(); err == nil {
		t.Fatal("bogus mode should fail validation loudly")
	}
}
