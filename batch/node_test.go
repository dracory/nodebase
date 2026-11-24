package batch

// Pack-local unit tests for the batch node — count/concat/interval
// modes, overlap, honourParts and validation, run against a fake host.

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
// emitted-message capture for the interval flush.
type fakeHost struct {
	states  map[string]interface{}
	mu      sync.Mutex // guards emitted — the interval flush emits from a timer goroutine
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
	key := wf + "/" + nodeID
	if s, ok := h.states[key]; ok {
		return s
	}
	s := f()
	h.states[key] = s
	return s
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

func newNode(cfg *BatchConfig) (*BatchNode, *fakeHost) {
	host := newFakeHost()
	return NewBatchNode(nodestore.NewNode("b1", "batch").SetWorkflowID("wf"), cfg, host), host
}

func TestBatchCount(t *testing.T) {
	n, _ := newNode(&BatchConfig{Mode: "count", Count: 2})
	ctx := context.Background()
	out, err := n.Execute(ctx, nodestore.NewMessage().SetPayload("a"))
	if err != nil || out != nil {
		t.Fatal("first message should buffer")
	}
	out, err = n.Execute(ctx, nodestore.NewMessage().SetPayload("b"))
	if err != nil || out == nil {
		t.Fatal("batch should complete at count")
	}
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "a" || arr[1] != "b" {
		t.Fatalf("batch payload wrong: %v", out.GetPayload())
	}
}

func TestBatchCountOverlap(t *testing.T) {
	n, _ := newNode(&BatchConfig{Mode: "count", Count: 2, Overlap: 1})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("a"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("b"))
	// overlap keeps "b" → next batch needs only one more
	out, _ := n.Execute(ctx, nodestore.NewMessage().SetPayload("c"))
	arr := out.GetPayload().([]interface{})
	if len(arr) != 2 || arr[0] != "b" || arr[1] != "c" {
		t.Fatalf("overlap batch wrong: %v", arr)
	}
}

func TestBatchHonourParts(t *testing.T) {
	n, _ := newNode(&BatchConfig{Mode: "count", Count: 1, HonourParts: true})
	ctx := context.Background()
	mk := func(id string, v interface{}) nodestore.MessageInterface {
		m := nodestore.NewMessage().SetPayload(v)
		m.SetMetadata(map[string]interface{}{
			"parts": map[string]interface{}{"id": id, "index": 0, "count": 1},
		})
		return m
	}
	out1, _ := n.Execute(ctx, mk("g1", "x"))
	out2, _ := n.Execute(ctx, mk("g2", "y"))
	// separate buffers per parts.id — each completes independently
	if out1 == nil || out2 == nil {
		t.Fatal("honourParts should batch each group separately")
	}
}

func TestBatchConcat(t *testing.T) {
	n, _ := newNode(&BatchConfig{Mode: "concat"})
	ctx := context.Background()
	mk := func(i int, v interface{}) nodestore.MessageInterface {
		m := nodestore.NewMessage().SetPayload(v)
		m.SetMetadata(map[string]interface{}{
			"parts": map[string]interface{}{"id": "g", "index": i, "count": 3},
		})
		return m
	}
	if out, _ := n.Execute(ctx, mk(0, []interface{}{1.0, 2.0})); out != nil {
		t.Fatal("incomplete concat should buffer")
	}
	n.Execute(ctx, mk(1, []interface{}{3.0}))
	out, _ := n.Execute(ctx, mk(2, "tail"))
	arr := out.GetPayload().([]interface{})
	// arrays flattened, scalar appended
	if len(arr) != 4 || arr[0] != 1.0 || arr[3] != "tail" {
		t.Fatalf("concat wrong: %v", arr)
	}
}

func TestBatchConcatNoParts(t *testing.T) {
	n, _ := newNode(&BatchConfig{Mode: "concat"})
	msg := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), msg)
	if err != nil || out != msg {
		t.Fatal("message without parts should pass through")
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
	deadline := time.Now().Add(2 * time.Second)
	for len(host.emittedMsgs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	emitted := host.emittedMsgs()
	if len(emitted) != 1 {
		t.Fatal("window flush should emit one batch")
	}
	arr := emitted[0].GetPayload().([]interface{})
	if len(arr) != 2 {
		t.Fatalf("interval batch wrong: %v", arr)
	}
}

func TestBatchValidate(t *testing.T) {
	n, _ := newNode(&BatchConfig{Mode: "count"})
	if err := n.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	// NOTE: mode() normalizes unknown values to "count", so Validate can
	// never fire on a bogus mode (documented; flagged in task evidence).
	bad, _ := newNode(&BatchConfig{Mode: "bogus"})
	if err := bad.Validate(); err != nil {
		t.Fatalf("bogus mode degrades to count, should validate: %v", err)
	}
}
