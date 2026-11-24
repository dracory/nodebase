package linkcall

// Pack-local unit tests for the linkCall node — target resolution,
// call-stack push, response await and timeout/discard paths.

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost resolves linkIn targets and captures injections.
type fakeHost struct {
	nodes      map[string]nodestore.NodeInterface
	injected   nodestore.MessageInterface
	injectErr  error
	responseCh chan nodestore.MessageInterface
	discarded  string
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	if h.injectErr != nil {
		return "", nil, h.injectErr
	}
	h.injected = p
	return "pending-1", h.responseCh, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error { return nil }
func (h *fakeHost) DiscardPending(id string)                                      { h.discarded = id }
func (h *fakeHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) {
	n, ok := h.nodes[nodeID]
	return n, ok
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

func newNode(cfg *LinkCallConfig, host *fakeHost) *LinkCallNode {
	return NewLinkCallNode(nodestore.NewNode("lc1", "linkCall").SetWorkflowID("wf"), cfg, host)
}

func newHost() *fakeHost {
	return &fakeHost{
		nodes:      map[string]nodestore.NodeInterface{"li1": nodestore.NewNode("li1", "linkIn")},
		responseCh: make(chan nodestore.MessageInterface, 1),
	}
}

func TestLinkCallRoundTrip(t *testing.T) {
	host := newHost()
	n := newNode(&LinkCallConfig{Link: "li1", Timeout: 2}, host)

	resp := nodestore.NewMessage().SetPayload("answer")
	host.responseCh <- resp

	in := nodestore.NewMessage().SetPayload("request")
	out, err := n.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != resp {
		t.Fatal("response not returned")
	}
	// the injected message carries the pushed call stack
	stack, _ := host.injected.GetMetadata()[nodebase.LinkCallStackKey].([]interface{})
	if len(stack) != 1 || stack[0].(map[string]interface{})["nodeId"] != "lc1" {
		t.Fatalf("call stack wrong: %v", stack)
	}
}

func TestLinkCallDynamicTarget(t *testing.T) {
	host := newHost()
	host.responseCh <- nodestore.NewMessage()
	n := newNode(&LinkCallConfig{LinkType: "dynamic", Timeout: 1}, host)
	in := nodestore.NewMessage().SetMetadata(map[string]interface{}{"target": "li1"})
	if _, err := n.Execute(context.Background(), in); err != nil {
		t.Fatalf("dynamic target failed: %v", err)
	}
}

func TestLinkCallTargetErrors(t *testing.T) {
	host := newHost()
	host.nodes["notlink"] = nodestore.NewNode("x", "debug")

	// no target at all
	n := newNode(&LinkCallConfig{}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("missing target should fail")
	}
	// unknown target
	n2 := newNode(&LinkCallConfig{Link: "ghost"}, host)
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("unknown target should fail")
	}
	// wrong type
	n3 := newNode(&LinkCallConfig{Link: "notlink"}, host)
	if _, err := n3.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("non-linkIn target should fail")
	}
	// inject failure
	host.injectErr = errors.New("no route")
	n4 := newNode(&LinkCallConfig{Link: "li1"}, host)
	if _, err := n4.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("inject failure should propagate")
	}
}

func TestLinkCallTimeout(t *testing.T) {
	host := newHost()
	n := newNode(&LinkCallConfig{Link: "li1", Timeout: 0.05}, host)
	start := time.Now()
	_, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err == nil {
		t.Fatal("no response should time out")
	}
	if time.Since(start) > time.Second {
		t.Fatal("timeout not honored")
	}
	if host.discarded != "pending-1" {
		t.Fatal("pending correlation not discarded on timeout")
	}
}
