package rbe

// Pack-local unit tests for the rbe node — all comparison modes,
// septopics, reset, gap percentages and band edge cases, run against
// a fake host (no engine).

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost with an in-memory state slot.
type fakeHost struct {
	states map[string]interface{}
}

func newFakeHost() *fakeHost { return &fakeHost{states: map[string]interface{}{}} }

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

func newNode(cfg *RBEConfig) *RBENode {
	return NewRBENode(nodestore.NewNode("r1", "rbe").SetWorkflowID("wf"), cfg, newFakeHost())
}

func send(t *testing.T, n *RBENode, payload interface{}) bool {
	t.Helper()
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(payload))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	return out != nil
}

func TestRBEDefault(t *testing.T) {
	n := newNode(&RBEConfig{Func: "rbe"})
	if !send(t, n, 1.0) {
		t.Fatal("first message should pass")
	}
	if send(t, n, 1.0) {
		t.Fatal("unchanged value should be blocked")
	}
	if !send(t, n, 2.0) {
		t.Fatal("changed value should pass")
	}
}

func TestRBEIBlocksFirst(t *testing.T) {
	n := newNode(&RBEConfig{Func: "rbei"})
	if send(t, n, 1.0) {
		t.Fatal("rbei should block the first message")
	}
	if send(t, n, 1.0) {
		t.Fatal("rbei should block unchanged value")
	}
	if !send(t, n, 2.0) {
		t.Fatal("rbei should pass changed value")
	}
}

func TestRBEDeadband(t *testing.T) {
	n := newNode(&RBEConfig{Func: "deadband", Gap: "5"})
	if !send(t, n, 100.0) {
		t.Fatal("first message should pass")
	}
	if send(t, n, 103.0) {
		t.Fatal("within gap should be blocked")
	}
	if !send(t, n, 106.0) {
		t.Fatal("beyond gap should pass")
	}
	// deadbandEq passes at-or-beyond
	n2 := newNode(&RBEConfig{Func: "deadbandEq", Gap: "5"})
	send(t, n2, 100.0)
	if send(t, n2, 105.0) != true {
		t.Fatal("deadbandEq at boundary should pass")
	}
}

func TestRBENarrowband(t *testing.T) {
	n := newNode(&RBEConfig{Func: "narrowband", Gap: "5"})
	send(t, n, 100.0)
	if !send(t, n, 104.0) {
		t.Fatal("within gap should pass")
	}
	if send(t, n, 200.0) {
		t.Fatal("beyond gap should be blocked")
	}
	// narrowbandEq strictly within
	n2 := newNode(&RBEConfig{Func: "narrowbandEq", Gap: "5"})
	send(t, n2, 100.0)
	if send(t, n2, 105.0) {
		t.Fatal("narrowbandEq at boundary should block")
	}
}

func TestRBEGapPercent(t *testing.T) {
	n := newNode(&RBEConfig{Func: "deadband", Gap: "10%"})
	send(t, n, 100.0)
	if send(t, n, 105.0) {
		t.Fatal("within 10% should block")
	}
	if !send(t, n, 120.0) {
		t.Fatal("beyond 10% should pass")
	}
}

func TestRBENonNumericBand(t *testing.T) {
	n := newNode(&RBEConfig{Func: "deadband", Gap: "5"})
	_, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("abc"))
	if err == nil {
		t.Fatal("non-numeric in band mode should error")
	}
}

func TestRBEMissingProperty(t *testing.T) {
	n := newNode(&RBEConfig{Property: "v"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{"x": 1}))
	if err != nil || out != nil {
		t.Fatal("missing watched property should drop")
	}
}

func TestRBESepTopics(t *testing.T) {
	n := newNode(&RBEConfig{Func: "rbe", SepTopics: true})
	mk := func(topic string, v interface{}) nodestore.MessageInterface {
		m := nodestore.NewMessage().SetPayload(v)
		m.SetMetadata(map[string]interface{}{"topic": topic})
		return m
	}
	ctx := context.Background()
	if out, _ := n.Execute(ctx, mk("a", 1.0)); out == nil {
		t.Fatal("topic a first should pass")
	}
	if out, _ := n.Execute(ctx, mk("b", 1.0)); out == nil {
		t.Fatal("topic b first should pass (separate state)")
	}
	if out, _ := n.Execute(ctx, mk("a", 1.0)); out != nil {
		t.Fatal("topic a unchanged should block")
	}
}

func TestRBEReset(t *testing.T) {
	n := newNode(&RBEConfig{Func: "rbe"})
	send(t, n, 1.0)
	m := nodestore.NewMessage().SetPayload(1.0)
	m.SetMetadata(map[string]interface{}{"reset": true})
	out, err := n.Execute(context.Background(), m)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out == nil {
		t.Fatal("after reset the same value should pass again")
	}
}

func TestRBEInitialValue(t *testing.T) {
	host := newFakeHost()
	n := NewRBENode(nodestore.NewNode("r2", "rbe").SetWorkflowID("wf"),
		&RBEConfig{Func: "rbe", InitialValue: `1`}, host)
	// prev seeded to 1 → first message with 1 is blocked
	if send(t, n, 1.0) {
		t.Fatal("seeded initial value should block the first message")
	}
	if !send(t, n, 2.0) {
		t.Fatal("changed value should pass")
	}
}
