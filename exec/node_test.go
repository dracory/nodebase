package exec

// Pack-local unit tests for the exec node — the AllowExec gate,
// command assembly, the three Node-RED output ports (stdout, stderr,
// return code) and spawn/kill semantics. Commands run through the
// platform shell (echo/false are universally available).

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost; it records Emits per port so
// the multi-output contract can be asserted.
type fakeHost struct {
	allow bool
	mu    sync.Mutex
	emits map[string][]nodestore.MessageInterface
	done  chan struct{}
}

func newFakeHost(allow bool) *fakeHost {
	return &fakeHost{allow: allow, emits: map[string][]nodestore.MessageInterface{}, done: make(chan struct{})}
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	for _, port := range ports {
		h.emits[port] = append(h.emits[port], p)
	}
	if len(h.emits["output_3"]) > 0 && h.done != nil {
		select {
		case <-h.done:
		default:
			close(h.done)
		}
	}
	h.mu.Unlock()
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

// RunningFlow reports true so port-scoped Emits are recorded —
// Execute skips them only when no flow is running.
func (h *fakeHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) { return nil, true }
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
func (h *fakeHost) ExecAllowed() bool                                   { return h.allow }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

func newNode(cfg *ExecConfig, allow bool) *ExecNode {
	return NewExecNode(nodestore.NewNode("e1", "exec"), cfg, newFakeHost(allow))
}

func TestExecDisabled(t *testing.T) {
	n := newNode(&ExecConfig{Command: "echo hi"}, false)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("exec should be gated by ExecAllowed")
	}
	// nil host is also disabled
	n2 := NewExecNode(nodestore.NewNode("e2", "exec"), &ExecConfig{Command: "echo hi"}, nil)
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("nil host should be disabled")
	}
}

func TestExecEmptyCommand(t *testing.T) {
	n := newNode(&ExecConfig{Command: "  "}, true)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("empty command should error")
	}
}

func TestExecRuns(t *testing.T) {
	h := newFakeHost(true)
	n := NewExecNode(nodestore.NewNode("e1", "exec"), &ExecConfig{Command: "echo hello"}, h)
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if !strings.Contains(out.GetPayload().(string), "hello") {
		t.Fatalf("stdout wrong: %v", out.GetPayload())
	}
	// Node-RED parity: msg.rc carries the third-output payload {code:0}
	rc, ok := out.GetMetadata()["rc"].(map[string]interface{})
	if !ok || rc["code"] != 0 {
		t.Fatalf("rc wrong: %v", out.GetMetadata())
	}
	if ports := n.SelectPorts(nil, out); ports[0] != "output_1" {
		t.Fatalf("stdout should route to output_1: %v", ports)
	}
	// {code:0} must land on output_3
	if got := h.emits["output_3"]; len(got) != 1 {
		t.Fatalf("expected one output_3 emit, got %v", got)
	} else if p, ok := got[0].GetPayload().(map[string]interface{}); !ok || p["code"] != 0 {
		t.Fatalf("output_3 payload wrong: %v", got[0].GetPayload())
	}
	if len(h.emits["output_2"]) != 0 {
		t.Fatal("no stderr expected on output_2")
	}
}

func TestExecNonZeroRC(t *testing.T) {
	h := newFakeHost(true)
	n := NewExecNode(nodestore.NewNode("e1", "exec"), &ExecConfig{Command: "echo oops >&2; exit 3"}, h)
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("non-zero rc is not an Execute error: %v", err)
	}
	// stdout is empty but still returned on output_1
	if ports := n.SelectPorts(nil, out); ports[0] != "output_1" {
		t.Fatalf("stdout should route to output_1: %v", ports)
	}
	// stderr lands on output_2
	if got := h.emits["output_2"]; len(got) != 1 || !strings.Contains(got[0].GetPayload().(string), "oops") {
		t.Fatalf("stderr should reach output_2: %v", h.emits)
	}
	// the return object lands on output_3 with code + Node's wording
	got := h.emits["output_3"]
	if len(got) != 1 {
		t.Fatalf("expected one output_3 emit, got %v", got)
	}
	p, ok := got[0].GetPayload().(map[string]interface{})
	if !ok || p["code"] != 3 {
		t.Fatalf("output_3 code wrong: %v", p)
	}
	if m, _ := p["message"].(string); !strings.HasPrefix(m, "Command failed: ") {
		t.Fatalf("output_3 message wrong: %v", p)
	}
}

func TestExecAddPayAppend(t *testing.T) {
	h := newFakeHost(true)
	n := NewExecNode(nodestore.NewNode("e1", "exec"), &ExecConfig{Command: "echo", AddPay: "payload", Append: "tail"}, h)
	msg := nodestore.NewMessage().SetPayload("mid")
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	got := out.GetPayload().(string)
	if !strings.Contains(got, "mid") || !strings.Contains(got, "tail") {
		t.Fatalf("command assembly wrong: %q", got)
	}
}

func TestExecAddPayTrue(t *testing.T) {
	// Node-RED serializes "append msg.payload" as addpay:true.
	h := newFakeHost(true)
	n := NewExecNode(nodestore.NewNode("e1", "exec"), &ExecConfig{Command: "echo", AddPay: true}, h)
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("viapayload"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if !strings.Contains(out.GetPayload().(string), "viapayload") {
		t.Fatalf("addpay:true should append msg.payload: %q", out.GetPayload())
	}
}

func TestExecSpawnStreamsAndReportsCode(t *testing.T) {
	h := newFakeHost(true)
	n := NewExecNode(nodestore.NewNode("e1", "exec"), &ExecConfig{Command: `sh -c "echo hi; exit 0"`, UseSpawn: true}, h)
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != nil {
		t.Fatal("spawn mode returns no synchronous output")
	}
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("spawn close event never reached output_3")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var sawHi bool
	for _, m := range h.emits["output_1"] {
		if strings.Contains(m.GetPayload().(string), "hi") {
			sawHi = true
		}
	}
	if !sawHi {
		t.Fatalf("stdout chunk missing: %v", h.emits)
	}
	p, ok := h.emits["output_3"][0].GetPayload().(map[string]interface{})
	if !ok || p["code"] != 0 {
		t.Fatalf("output_3 payload wrong: %v", h.emits["output_3"])
	}
}

func TestExecKillSignal(t *testing.T) {
	h := newFakeHost(true)
	n := NewExecNode(nodestore.NewNode("e1", "exec"), &ExecConfig{Command: `sh -c "sleep 30"`, UseSpawn: true}, h)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("spawn failed: %v", err)
	}
	// Wait until the child is registered, then send a kill message.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.procState().mu.Lock()
		count := len(n.procState().procs)
		n.procState().mu.Unlock()
		if count > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	kill := nodestore.NewMessage()
	kill.SetMetadata(map[string]interface{}{"kill": "SIGTERM"})
	out, err := n.Execute(context.Background(), kill)
	if err != nil || out != nil {
		t.Fatalf("kill should be consumed silently: out=%v err=%v", out, err)
	}
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("kill never produced the output_3 close event")
	}
	p, _ := h.emits["output_3"][0].GetPayload().(map[string]interface{})
	if p["code"] != nil {
		t.Fatalf("killed process should report code:null: %v", p)
	}
}

func TestExecValidate(t *testing.T) {
	if err := newNode(&ExecConfig{}, false).Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	if err := newNode(&ExecConfig{Timer: -1}, false).Validate(); err == nil {
		t.Fatal("negative timer should fail")
	}
}
