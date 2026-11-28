package function

// Pack-local unit tests for the function node — the goja script
// surface (msg mapping, node.send, node.error, context/env APIs,
// timeout, dispatch/port routing) run against a fake host.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost: state slot, memory context
// store, env map and emitted-message capture.
type fakeHost struct {
	mu         sync.Mutex
	states     map[string]interface{}
	emitted    []nodestore.MessageInterface
	events     []string
	env        map[string]interface{}
	store      nodebase.ContextStore
	runningWf  nodestore.WorkflowInterface
	downstream []downstreamCall
}

// downstreamCall records one ExecuteDownstream invocation — the path
// queued sends (multi-message returns, setTimeout callbacks) take.
type downstreamCall struct {
	from  string
	p     nodestore.MessageInterface
	ports []string
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		states: map[string]interface{}{},
		env:    map[string]interface{}{},
		store:  nodebase.NewMemoryContextStore(),
	}
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.emitted = append(h.emitted, p)
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
func (h *fakeHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) {
	if h.runningWf != nil {
		return h.runningWf, true
	}
	return nil, false
}
func (h *fakeHost) RunContext(wf string) context.Context { return context.Background() }
func (h *fakeHost) GetOrCreateNodeState(wf, nodeID string, f func() interface{}) interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()
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
	h.downstream = append(h.downstream, downstreamCall{from: from, p: p, ports: ports})
}

// downstreamCalls returns a snapshot of the recorded downstream sends —
// the drain goroutine appends while tests read.
func (h *fakeHost) downstreamCalls() []downstreamCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]downstreamCall(nil), h.downstream...)
}
func (h *fakeHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) {
	v, ok := h.env[name]
	return v, ok
}
func (h *fakeHost) ResolvedContextStore() nodebase.ContextStore        { return h.store }
func (h *fakeHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)  { return nil, nil }
func (h *fakeHost) FileRootPath() string                               { return "" }
func (h *fakeHost) ExecAllowed() bool                                  { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter              { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool) { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                { return nil }

var nodeSeq int

func newNode(script string, host *fakeHost) *FunctionNode {
	nodeSeq++
	return NewFunctionNode(
		nodestore.NewNode(fmt.Sprintf("fn%d", nodeSeq), "function").SetWorkflowID("wf"),
		&FunctionConfig{Func: script}, host)
}

// newNodeFor creates a node bound to a specific context scope id —
// needed when a test shares one host across two executors that must
// see the same context (context scope is keyed by node id).
func newNodeFor(id, script string, host *fakeHost) *FunctionNode {
	return NewFunctionNode(
		nodestore.NewNode(id, "function").SetWorkflowID("wf"),
		&FunctionConfig{Func: script}, host)
}

func msg(payload interface{}) nodestore.MessageInterface {
	return nodestore.NewMessage().SetID("in1").SetPayload(payload)
}

func TestFunctionReturnMsg(t *testing.T) {
	host := newFakeHost()
	n := newNode(`msg.payload = msg.payload * 2; return msg;`, host)
	out, err := n.Execute(context.Background(), msg(3.0))
	if err != nil || out == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if fmt.Sprintf("%v", out.GetPayload()) != "6" {
		t.Fatalf("payload wrong: %v", out.GetPayload())
	}
	// same msgid → the same message passed through
	if out.GetID() != "in1" {
		t.Fatalf("passthrough should keep msgid, got %q", out.GetID())
	}
}

func TestFunctionReturnNullDrops(t *testing.T) {
	host := newFakeHost()
	n := newNode(`return null;`, host)
	out, err := n.Execute(context.Background(), msg(1))
	if err != nil || out != nil {
		t.Fatal("null return should drop the message")
	}
}

func TestFunctionNodeError(t *testing.T) {
	host := newFakeHost()
	n := newNode(`node.error("boom");`, host)
	if _, err := n.Execute(context.Background(), msg(1)); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("node.error should abort: %v", err)
	}
}

func TestFunctionCompileError(t *testing.T) {
	host := newFakeHost()
	n := newNode(`this is not js (((`, host)
	if _, err := n.Execute(context.Background(), msg(1)); err == nil {
		t.Fatal("compile error should surface")
	}
}

func TestFunctionRuntimeError(t *testing.T) {
	host := newFakeHost()
	n := newNode(`nonexistent.call();`, host)
	if _, err := n.Execute(context.Background(), msg(1)); err == nil {
		t.Fatal("runtime error should surface")
	}
}

func TestFunctionMultiOutput(t *testing.T) {
	host := newFakeHost()
	n := newNode(`return [null, {payload: "p2"}];`, host)
	out, err := n.Execute(context.Background(), msg(1))
	if err != nil || out == nil {
		t.Fatalf("execute failed: %v", err)
	}
	// port 0 skipped (null); the port-1 message is returned and routed
	if out == nil || out.GetPayload() != "p2" {
		t.Fatalf("multi-output wrong: %v", out)
	}
	ports := n.SelectPorts(nil, out)
	if len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("ports wrong: %v", ports)
	}
}

func TestFunctionNodeSend(t *testing.T) {
	host := newFakeHost()
	n := newNode(`node.send([{payload: "a"}, {payload: "b"}]); return null;`, host)
	out, err := n.Execute(context.Background(), msg(1))
	if err != nil || out == nil {
		t.Fatalf("execute failed: %v", err)
	}
	// first send on lowest port returns; other port emitted async
	if out == nil || out.GetPayload() != "a" {
		t.Fatalf("returned message wrong: %v", out)
	}
	if len(host.emitted) != 1 || host.emitted[0].GetPayload() != "b" {
		t.Fatalf("port-2 message should be emitted: %v", host.emitted)
	}
}

func TestFunctionContext(t *testing.T) {
	host := newFakeHost()
	// Executors are rebuilt per message but share per-node state and
	// context scope — one script exercises set/get/keys and two
	// sequential executes prove context survives across them.
	n := newNodeFor("ctx-node", `
		if (context.get("count") === undefined) {
			context.set("count", 41); flow.set("shared", "x");
			msg.payload = context.get("count") + flow.get("shared");
		} else {
			msg.payload = context.keys();
		}
		return msg;`, host)
	out, err := n.Execute(context.Background(), msg(nil))
	if err != nil || out == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "41x" {
		t.Fatalf("context round-trip wrong: %v", out.GetPayload())
	}
	// executors are rebuilt per message in a real run — a fresh instance
	// with the same node id shares context scope but not send state
	n2 := newNodeFor("ctx-node", `
		if (context.get("count") === undefined) {
			context.set("count", 41); flow.set("shared", "x");
			msg.payload = context.get("count") + flow.get("shared");
		} else {
			msg.payload = context.keys();
		}
		return msg;`, host)
	out2, err := n2.Execute(context.Background(), msg(nil))
	if err != nil || out2 == nil {
		t.Fatalf("second execute failed: %v", err)
	}
	if fmt.Sprintf("%v", out2.GetPayload()) != "[count]" {
		t.Fatalf("context.keys wrong: %v", out2.GetPayload())
	}
}

func TestFunctionEnv(t *testing.T) {
	host := newFakeHost()
	host.env["MY_VAR"] = "seekrit"
	n := newNode(`msg.payload = env.get("MY_VAR"); return msg;`, host)
	out, err := n.Execute(context.Background(), msg(nil))
	if err != nil || out == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "seekrit" {
		t.Fatalf("env.get wrong: %v", out.GetPayload())
	}
	// missing env + no OSEnvAllowed → undefined (fails closed)
	n2 := newNode(`msg.payload = String(env.get("NOPE") === undefined); return msg;`, host)
	out2, err := n2.Execute(context.Background(), msg(nil))
	if err != nil || out2 == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out2.GetPayload() != "true" {
		t.Fatal("env.get should fail closed without OSEnvAllowed")
	}
}

func TestFunctionWarnLog(t *testing.T) {
	host := newFakeHost()
	n := newNode(`node.warn("w1"); node.log("l1"); return null;`, host)
	_, _ = n.Execute(context.Background(), msg(1))
	if len(host.events) != 2 || !strings.HasPrefix(host.events[0], "warn:") || !strings.HasPrefix(host.events[1], "info:") {
		t.Fatalf("warn/log events wrong: %v", host.events)
	}
}

func TestFunctionTimeout(t *testing.T) {
	host := newFakeHost()
	n := NewFunctionNode(nodestore.NewNode("fn2", "function").SetWorkflowID("wf"),
		&FunctionConfig{Func: `while(true){}`, Timeout: 50}, host)
	if _, err := n.Execute(context.Background(), msg(1)); err == nil {
		t.Fatal("infinite loop should be interrupted")
	}
}

func TestFunctionValidate(t *testing.T) {
	host := newFakeHost()
	if err := newNode(`return msg;`, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad := NewFunctionNode(nodestore.NewNode("f", "function"), &FunctionConfig{}, host)
	if err := bad.Validate(); err == nil {
		t.Fatal("empty func should fail validation")
	}
	bad2 := NewFunctionNode(nodestore.NewNode("f", "function"), &FunctionConfig{Func: "x", Outputs: -1}, host)
	if err := bad2.Validate(); err == nil {
		t.Fatal("negative outputs should fail validation")
	}
}
