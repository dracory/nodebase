package sort

// Pack-local unit tests for the sort node — msg and seq targets,
// compare semantics and validation, run against a fake host.

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

// fakeHost implements nodebase.NodeHost: state slot, captured
// ExecuteDownstream calls and a running workflow for emitSequence.
type fakeHost struct {
	states     map[string]interface{}
	mu         sync.Mutex // guards downstream — seq emission runs on a goroutine
	downstream []nodestore.MessageInterface
	workflow   nodestore.WorkflowInterface
}

func newFakeHost() *fakeHost {
	return &fakeHost{states: map[string]interface{}{}, workflow: nodestore.NewWorkflow("wf")}
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
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
	return h.workflow, true
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
	h.downstream = append(h.downstream, p)
}

func (h *fakeHost) downstreamMsgs() []nodestore.MessageInterface {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nodestore.MessageInterface(nil), h.downstream...)
}
func (h *fakeHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) { return nil, false }
func (h *fakeHost) ResolvedContextStore() nodebase.ContextStore         { return nil }
func (h *fakeHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)   { return nil, nil }
func (h *fakeHost) FileRootPath() string                                { return "" }
func (h *fakeHost) ExecAllowed() bool                                   { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

func newNode(cfg *SortConfig) (*SortNode, *fakeHost) {
	host := newFakeHost()
	return NewSortNode(nodestore.NewNode("s1", "sort").SetWorkflowID("wf"), cfg, host), host
}

func TestSortMsgAscDesc(t *testing.T) {
	n, _ := newNode(&SortConfig{})
	msg := nodestore.NewMessage().SetPayload([]interface{}{3.0, 1.0, 2.0})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	arr := msg.GetPayload().([]interface{})
	if arr[0] != 1.0 || arr[2] != 3.0 {
		t.Fatalf("asc sort wrong: %v", arr)
	}

	n2, _ := newNode(&SortConfig{Order: "desc"})
	msg2 := nodestore.NewMessage().SetPayload([]interface{}{1.0, 3.0, 2.0})
	n2.Execute(context.Background(), msg2)
	arr2 := msg2.GetPayload().([]interface{})
	if arr2[0] != 3.0 {
		t.Fatalf("desc sort wrong: %v", arr2)
	}
}

func TestSortMsgNonArray(t *testing.T) {
	n, _ := newNode(&SortConfig{})
	msg := nodestore.NewMessage().SetPayload("scalar")
	out, err := n.Execute(context.Background(), msg)
	if err != nil || out != msg {
		t.Fatal("non-array should pass through")
	}
}

func TestSortStringsAndMixed(t *testing.T) {
	n, _ := newNode(&SortConfig{})
	msg := nodestore.NewMessage().SetPayload([]interface{}{"b", "a", "c"})
	n.Execute(context.Background(), msg)
	arr := msg.GetPayload().([]interface{})
	if arr[0] != "a" || arr[2] != "c" {
		t.Fatalf("string sort wrong: %v", arr)
	}
	// asNum coerces numeric strings
	n2, _ := newNode(&SortConfig{AsNum: true})
	msg2 := nodestore.NewMessage().SetPayload([]interface{}{"10", "2", "1"})
	n2.Execute(context.Background(), msg2)
	arr2 := msg2.GetPayload().([]interface{})
	if arr2[0] != "1" || arr2[2] != "10" {
		t.Fatalf("asNum sort wrong: %v", arr2)
	}
}

func partMsg(id string, index, count int, payload interface{}) nodestore.MessageInterface {
	m := nodestore.NewMessage().SetPayload(payload)
	m.SetMetadata(map[string]interface{}{
		"parts": map[string]interface{}{"id": id, "index": index, "count": count},
	})
	return m
}

func TestSortSeq(t *testing.T) {
	n, host := newNode(&SortConfig{Target: "seq"})
	ctx := context.Background()

	// arrive out of order; buffered until count reached
	out, _ := n.Execute(ctx, partMsg("g", 2, 3, "c"))
	if out != nil {
		t.Fatal("incomplete seq should drop")
	}
	n.Execute(ctx, partMsg("g", 0, 3, "a"))
	n.Execute(ctx, partMsg("g", 1, 3, "b"))

	deadline := time.Now().Add(2 * time.Second)
	for len(host.downstreamMsgs()) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	downstream := host.downstreamMsgs()
	if len(downstream) != 3 {
		t.Fatalf("expected 3 re-emitted messages, got %d", len(downstream))
	}
	if downstream[0].GetPayload() != "a" || downstream[2].GetPayload() != "c" {
		t.Fatalf("seq order wrong: %v %v", downstream[0].GetPayload(), downstream[2].GetPayload())
	}
	// parts.index renumbered to sorted position
	pm := downstream[0].GetMetadata()["parts"].(map[string]interface{})
	if pm["index"] != 0 || pm["count"] != 3 {
		t.Fatalf("parts not renumbered: %v", pm)
	}
}

func TestSortSeqNoParts(t *testing.T) {
	n, _ := newNode(&SortConfig{Target: "seq"})
	msg := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), msg)
	if err != nil || out != msg {
		t.Fatal("message without parts should pass through")
	}
}

func TestSortSeqByKey(t *testing.T) {
	n, host := newNode(&SortConfig{Target: "seq", SeqKey: "rank"})
	ctx := context.Background()
	mk := func(i int, rank float64) nodestore.MessageInterface {
		m := nodestore.NewMessage().SetPayload(map[string]interface{}{"rank": rank})
		m.SetMetadata(map[string]interface{}{
			"parts": map[string]interface{}{"id": "g", "index": i, "count": 2},
		})
		return m
	}
	n.Execute(ctx, mk(0, 2))
	n.Execute(ctx, mk(1, 1))
	deadline := time.Now().Add(2 * time.Second)
	for len(host.downstreamMsgs()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	downstream := host.downstreamMsgs()
	if len(downstream) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(downstream))
	}
	first := downstream[0].GetPayload().(map[string]interface{})
	if first["rank"] != 1.0 {
		t.Fatalf("seqKey sort wrong: %v", first)
	}
}

func TestSortValidate(t *testing.T) {
	n, _ := newNode(&SortConfig{Target: "msg"})
	if err := n.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	// NOTE: target() normalizes unknown values to "msg", so Validate can
	// never fire on a bogus target — it degrades instead of failing
	// (documented; flagged in task evidence).
	bad, _ := newNode(&SortConfig{Target: "bogus"})
	if err := bad.Validate(); err != nil {
		t.Fatalf("bogus target degrades to msg, should validate: %v", err)
	}
}

func TestCompareValues(t *testing.T) {
	cases := []struct {
		a, b  interface{}
		asNum bool
		want  int
	}{
		{1.0, 2.0, false, -1},
		{2.0, 1.0, false, 1},
		{1.0, 1.0, false, 0},
		{"a", "b", false, -1},
		{"10", "2", true, 1},
		{"10", "2", false, -1}, // lexical
	}
	for _, c := range cases {
		if got := compareValues(c.a, c.b, c.asNum); got != c.want {
			t.Fatalf("compareValues(%v,%v,%v)=%d want %d", c.a, c.b, c.asNum, got, c.want)
		}
	}
}
