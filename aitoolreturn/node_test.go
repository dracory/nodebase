package aitoolreturn

// Pack-local unit tests for the ai-tool-return node: pending-call
// resolution via the stamped message id, and the not-a-tool-call
// error path.

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// hostStub captures ResolveResponse calls; the rest of NodeHost is
// unused by this node.
type hostStub struct {
	resolvedID  string
	resolvedMsg nodestore.MessageInterface
	err         error
}

func (h *hostStub) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *hostStub) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *hostStub) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *hostStub) ResolveResponse(id string, p nodestore.MessageInterface) error {
	if h.err != nil {
		return h.err
	}
	h.resolvedID, h.resolvedMsg = id, p
	return nil
}
func (h *hostStub) DiscardPending(id string)                                     {}
func (h *hostStub) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) { return nil, false }
func (h *hostStub) RunningFlow(wf string) (nodestore.WorkflowInterface, bool)    { return nil, false }
func (h *hostStub) RunContext(wf string) context.Context                         { return context.Background() }
func (h *hostStub) GetOrCreateNodeState(wf, nodeID string, f func() interface{}) interface{} {
	return f()
}
func (h *hostStub) ExecuteDownstream(ctx context.Context, wf, from string, w nodestore.WorkflowInterface, p nodestore.MessageInterface, ports []string) {
}
func (h *hostStub) NodeEnv(wf, nodeID, name string) (interface{}, bool) { return nil, false }
func (h *hostStub) ResolvedContextStore() nodebase.ContextStore         { return nil }
func (h *hostStub) LoadTLSConfig(wf, ref string) (*tls.Config, error)   { return nil, nil }
func (h *hostStub) FileRootPath() string                                { return "" }
func (h *hostStub) ExecAllowed() bool                                   { return false }
func (h *hostStub) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *hostStub) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *hostStub) QueueService() nodebase.QueueService                 { return nil }

func TestResolvesPendingCall(t *testing.T) {
	h := &hostStub{}
	n := NewReturnNode(nodestore.NewNode("r1", Type), &Config{}, h)
	msg := nodestore.NewMessage().SetPayload("tool result")
	msg.GetMetadata()["msgId"] = "msg-42"
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if h.resolvedID != "msg-42" || h.resolvedMsg != msg {
		t.Fatalf("resolved wrong call: %q", h.resolvedID)
	}
	if out != msg {
		t.Fatal("message must pass through")
	}
}

func TestNoPendingCorrelation(t *testing.T) {
	h := &hostStub{}
	n := NewReturnNode(nodestore.NewNode("r1", Type), &Config{}, h)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("expected missing-correlation error")
	}
}

func TestResolveErrorSurfaced(t *testing.T) {
	h := &hostStub{err: errors.New("no pending request")}
	n := NewReturnNode(nodestore.NewNode("r1", Type), &Config{}, h)
	msg := nodestore.NewMessage()
	msg.GetMetadata()["msgId"] = "gone"
	if _, err := n.Execute(context.Background(), msg); err == nil {
		t.Fatal("expected resolve error")
	}
}
