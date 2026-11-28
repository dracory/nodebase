package linkout

// Pack-local unit tests for the linkOut node — link-mode fan-out to
// linkIn targets and return-mode resolution of the linkCall stack.

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost records emits and lookup targets.
type fakeHost struct {
	nodes     map[string]nodestore.NodeInterface
	emitted   map[string]nodestore.MessageInterface
	resolved  string
	resolvedP nodestore.MessageInterface
	emitErr   error
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	if h.emitErr != nil {
		return h.emitErr
	}
	h.emitted[nodeID] = p
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error {
	h.resolved = id
	h.resolvedP = p
	return nil
}
func (h *fakeHost) DiscardPending(id string) {}
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

func newNode(cfg *LinkOutConfig, host *fakeHost) *LinkOutNode {
	return NewLinkOutNode(nodestore.NewNode("lo1", "linkOut").SetWorkflowID("wf"), cfg, host)
}

func newHost() *fakeHost {
	return &fakeHost{
		nodes:   map[string]nodestore.NodeInterface{"li1": nodestore.NewNode("li1", "linkIn")},
		emitted: map[string]nodestore.MessageInterface{},
	}
}

func TestLinkOutLinkMode(t *testing.T) {
	host := newHost()
	n := newNode(&LinkOutConfig{Links: []string{"li1"}, Mode: "link"}, host)
	in := nodestore.NewMessage().SetPayload("data")
	out, err := n.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != in {
		t.Fatal("link mode should pass the message through")
	}
	push := host.emitted["li1"]
	if push == nil {
		t.Fatal("nothing emitted to linkIn")
	}
	if push.GetMetadata()["linkSource"] != "lo1" {
		t.Fatal("linkSource stamp missing")
	}
}

func TestLinkOutTargetChecks(t *testing.T) {
	host := newHost()
	host.nodes["notlink"] = nodestore.NewNode("x", "debug")

	// Node-RED parity: dangling or non-linkIn targets are silently
	// skipped (no event listener), never an error — the message still
	// passes through on the node's own output.
	n := newNode(&LinkOutConfig{Links: []string{"ghost"}, Mode: "link"}, host)
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("unknown target should be skipped, not fail: %v", err)
	}
	if out == nil {
		t.Fatal("link mode should still pass the message through")
	}
	n2 := newNode(&LinkOutConfig{Links: []string{"notlink"}, Mode: "link"}, host)
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("non-linkIn target should be skipped, not fail: %v", err)
	}
}

func TestLinkOutReturnMode(t *testing.T) {
	host := newHost()
	n := newNode(&LinkOutConfig{Mode: "return"}, host)
	in := nodestore.NewMessage().SetPayload("result").SetMetadata(map[string]interface{}{
		nodebase.LinkCallStackKey: []interface{}{
			map[string]interface{}{"nodeId": "lc1", "msgId": "pending-1"},
		},
	})
	out, err := n.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("return failed: %v", err)
	}
	if out != in {
		t.Fatal("return mode should return the message")
	}
	if host.resolved != "pending-1" || host.resolvedP != in {
		t.Fatalf("pending response not resolved: %q", host.resolved)
	}
	stack := in.GetMetadata()[nodebase.LinkCallStackKey].([]interface{})
	if len(stack) != 0 {
		t.Fatal("call stack should be popped")
	}
}

func TestLinkOutReturnNoStack(t *testing.T) {
	host := newHost()
	n := newNode(&LinkOutConfig{Mode: "return"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("return without call stack should fail")
	}
}

func TestLinkOutConfigUnmarshal(t *testing.T) {
	var c LinkOutConfig
	if err := json.Unmarshal([]byte(`{"links":"a, b","mode":"link"}`), &c); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(c.Links) != 2 || c.Links[1] != "b" {
		t.Fatalf("links wrong: %v", c.Links)
	}
	var c2 LinkOutConfig
	_ = json.Unmarshal([]byte(`{"links":["x"]}`), &c2)
	if len(c2.Links) != 1 {
		t.Fatal("array links wrong")
	}
}
