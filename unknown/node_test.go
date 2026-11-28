package unknown

// Pack-local unit tests for the unknown node — message drop plus the
// once-per-run warning event.

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost captures EmitEvent calls.
type fakeHost struct {
	events []string
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {
	h.events = append(h.events, level+":"+message)
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

func TestUnknownDropsMessage(t *testing.T) {
	n := NewUnknownNode(nodestore.NewNode("u1", "mystery"), nil)
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil || out != nil {
		t.Fatal("unknown node should drop the message")
	}
}

func TestUnknownWarnsOnce(t *testing.T) {
	host := &fakeHost{}
	n := NewUnknownNode(nodestore.NewNode("u1", "mystery").SetWorkflowID("wf"), host)
	_, _ = n.Execute(context.Background(), nodestore.NewMessage())
	_, _ = n.Execute(context.Background(), nodestore.NewMessage())
	if len(host.events) != 1 {
		t.Fatalf("warning should fire once, got %v", host.events)
	}
	if host.events[0] != `warn:unknown node type "mystery" excluded from execution` {
		t.Fatalf("warning wrong: %v", host.events)
	}
}

func TestUnknownValidate(t *testing.T) {
	n := NewUnknownNode(nodestore.NewNode("u1", "mystery"), nil)
	if err := n.Validate(); err != nil {
		t.Fatalf("unknown config should validate: %v", err)
	}
}
