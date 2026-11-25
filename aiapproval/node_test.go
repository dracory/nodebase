package aiapproval

// Pack-local unit tests for the ai-approval node: request emission on
// output 2, approvalId correlation, reply resume, rejection and the
// restart path — a re-driven hop over the same context store finds
// the pending record and resumes on the stored decision.

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

// fakeHost implements the NodeHost surface the approval node needs:
// state slots (waiter table), the context store (pending records) and
// an Emit capture for the output-2 request.
type fakeHost struct {
	states  map[string]interface{}
	store   nodebase.ContextStore
	mu      sync.Mutex
	emitted []nodestore.MessageInterface
}

func newFakeHost() *fakeHost {
	return &fakeHost{states: map[string]interface{}{}, store: nodebase.NewMemoryContextStore()}
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error { return nil }
func (h *fakeHost) DiscardPending(id string)                                      {}
func (h *fakeHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool)  { return nil, false }
func (h *fakeHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool)     { return nil, false }
func (h *fakeHost) RunContext(wf string) context.Context                          { return context.Background() }
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
func (h *fakeHost) ResolvedContextStore() nodebase.ContextStore         { return h.store }
func (h *fakeHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)   { return nil, nil }
func (h *fakeHost) FileRootPath() string                                { return "" }
func (h *fakeHost) ExecAllowed() bool                                   { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

func request(msgID string) nodestore.MessageInterface {
	m := nodestore.NewMessage().SetPayload("delete everything?")
	m.GetMetadata()["msgId"] = msgID
	return m
}

func reply(approvalID string, approved bool) nodestore.MessageInterface {
	m := nodestore.NewMessage()
	m.GetMetadata()[ReplyKey] = map[string]interface{}{
		"approvalId": approvalID, "approved": approved, "comment": "ok",
	}
	return m
}

// waitEmit polls until the output-2 request lands, returning its
// approvalId.
func waitEmit(t *testing.T, h *fakeHost) string {
	t.Helper()
	for i := 0; i < 200; i++ {
		h.mu.Lock()
		if len(h.emitted) > 0 {
			id, _ := h.emitted[0].GetMetadata()["approvalId"].(string)
			h.mu.Unlock()
			return id
		}
		h.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("approval request never emitted on output 2")
	return ""
}

func TestRequestReplyRoundTrip(t *testing.T) {
	h := newFakeHost()
	n := NewApprovalNode(nodestore.NewNode("ap", Type), &Config{TimeoutSeconds: 5}, h)

	done := make(chan nodestore.MessageInterface, 1)
	go func() {
		out, err := n.Execute(context.Background(), request("msg-1"))
		if err != nil {
			t.Errorf("request failed: %v", err)
		}
		done <- out
	}()

	approvalID := waitEmit(t, h)
	if approvalID != "appr-msg-1" {
		t.Fatalf("approvalId wrong: %q", approvalID)
	}

	out, err := n.Execute(context.Background(), reply(approvalID, true))
	if err != nil {
		t.Fatalf("reply failed: %v", err)
	}
	if ports := n.SelectPorts(out, out); len(ports) != 0 {
		t.Fatal("reply message must be consumed (no ports)")
	}

	resumed := <-done
	decision, _ := resumed.GetMetadata()[DecisionKey].(map[string]interface{})
	if decision["approved"] != true || decision["approvalId"] != approvalID {
		t.Fatalf("decision metadata wrong: %+v", decision)
	}
	if ports := n.SelectPorts(resumed, resumed); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("resumed message must route to output_1: %v", ports)
	}
}

func TestRejectedResume(t *testing.T) {
	h := newFakeHost()
	n := NewApprovalNode(nodestore.NewNode("ap", Type), &Config{TimeoutSeconds: 5}, h)
	done := make(chan nodestore.MessageInterface, 1)
	go func() {
		out, _ := n.Execute(context.Background(), request("msg-2"))
		done <- out
	}()
	approvalID := waitEmit(t, h)
	if _, err := n.Execute(context.Background(), reply(approvalID, false)); err != nil {
		t.Fatalf("reply failed: %v", err)
	}
	resumed := <-done
	decision, _ := resumed.GetMetadata()[DecisionKey].(map[string]interface{})
	if decision["approved"] != false {
		t.Fatalf("rejection not propagated: %+v", decision)
	}
}

// TestRestartDecidedRecord simulates a restart after the reply landed
// but before the waiting hop resumed: a rebuilt node over the same
// store sees the decided record and resumes without waiting.
func TestRestartDecidedRecord(t *testing.T) {
	shared := nodebase.NewMemoryContextStore()

	// Pre-seed a decided record as if the reply landed pre-restart.
	rec := record{Question: "q", Status: "approved", Comment: "go"}
	if err := shared.SetContext(nodebase.ContextScopeGlobal, scopePrefix+"ap", "appr-msg-3", rec); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	h2 := newFakeHost()
	h2.store = shared // durable backend survives the restart
	n2 := NewApprovalNode(nodestore.NewNode("ap", Type), &Config{TimeoutSeconds: 5}, h2)
	out, err := n2.Execute(context.Background(), request("msg-3"))
	if err != nil {
		t.Fatalf("restart resume failed: %v", err)
	}
	decision, _ := out.GetMetadata()[DecisionKey].(map[string]interface{})
	if decision["approved"] != true {
		t.Fatalf("decision lost across restart: %+v", decision)
	}
	// No request is re-emitted for a decided approval.
	h2.mu.Lock()
	defer h2.mu.Unlock()
	if len(h2.emitted) != 0 {
		t.Fatal("decided approval must not re-emit a request")
	}
}

// TestRestartPendingThenReply: the hop is re-driven after restart
// while the approval is still pending — the node re-announces the
// request and resumes once the reply arrives.
func TestRestartPendingThenReply(t *testing.T) {
	shared := nodebase.NewMemoryContextStore()
	if err := shared.SetContext(nodebase.ContextScopeGlobal, scopePrefix+"ap", "appr-msg-4",
		record{Question: "q", Status: "pending"}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}
	h := newFakeHost()
	h.store = shared
	n := NewApprovalNode(nodestore.NewNode("ap", Type), &Config{TimeoutSeconds: 5}, h)
	done := make(chan nodestore.MessageInterface, 1)
	go func() {
		out, err := n.Execute(context.Background(), request("msg-4"))
		if err != nil {
			t.Errorf("re-driven request failed: %v", err)
		}
		done <- out
	}()
	approvalID := waitEmit(t, h)
	if approvalID != "appr-msg-4" {
		t.Fatalf("re-driven approvalId changed: %q", approvalID)
	}
	if _, err := n.Execute(context.Background(), reply(approvalID, true)); err != nil {
		t.Fatalf("reply failed: %v", err)
	}
	if out := <-done; out == nil {
		t.Fatal("no resume after reply")
	}
}

func TestTimeout(t *testing.T) {
	h := newFakeHost()
	n := NewApprovalNode(nodestore.NewNode("ap", Type), &Config{TimeoutSeconds: 0.05}, h)
	if _, err := n.Execute(context.Background(), request("msg-5")); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestReplyUnknownApprovalStillRecorded(t *testing.T) {
	h := newFakeHost()
	n := NewApprovalNode(nodestore.NewNode("ap", Type), &Config{}, h)
	out, err := n.Execute(context.Background(), reply("appr-unknown", true))
	if err != nil {
		t.Fatalf("reply failed: %v", err)
	}
	// Recorded in the store so a later-arriving (re-driven) request
	// resumes immediately.
	raw, ok, _ := h.store.GetContext(nodebase.ContextScopeGlobal, scopePrefix+"ap", "appr-unknown")
	if !ok {
		t.Fatal("reply decision not persisted")
	}
	rec := decodeRecord(raw)
	if rec == nil || rec.Status != "approved" {
		t.Fatalf("stored decision wrong: %+v", raw)
	}
	_ = out
}
