package exec

// Pack-local unit tests for the exec node — the AllowExec gate,
// command assembly, rc port routing and validation. Commands run
// through the platform shell (echo/false are universally available).

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost; only ExecAllowed matters.
type fakeHost struct {
	allow bool
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
func (h *fakeHost) ExecAllowed() bool                                   { return h.allow }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

func newNode(cfg *ExecConfig, allow bool) *ExecNode {
	return NewExecNode(nodestore.NewNode("e1", "exec"), cfg, &fakeHost{allow: allow})
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
	n := newNode(&ExecConfig{Command: "echo hello"}, true)
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if !strings.Contains(out.GetPayload().(string), "hello") {
		t.Fatalf("stdout wrong: %v", out.GetPayload())
	}
	if out.GetMetadata()["rc"] != 0 {
		t.Fatalf("rc wrong: %v", out.GetMetadata())
	}
	if ports := n.SelectPorts(nil, out); ports[0] != "output_1" {
		t.Fatalf("rc 0 should route to output_1: %v", ports)
	}
}

func TestExecNonZeroRC(t *testing.T) {
	n := newNode(&ExecConfig{Command: "exit 3"}, true)
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("non-zero rc is not an Execute error: %v", err)
	}
	if out.GetMetadata()["rc"] != 3 {
		t.Fatalf("rc wrong: %v", out.GetMetadata())
	}
	if ports := n.SelectPorts(nil, out); ports[0] != "output_3" {
		t.Fatalf("non-zero rc should route to output_3: %v", ports)
	}
}

func TestExecAddPayAppend(t *testing.T) {
	n := newNode(&ExecConfig{Command: "echo", AddPay: "payload", Append: "tail"}, true)
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

func TestExecValidate(t *testing.T) {
	if err := newNode(&ExecConfig{}, false).Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	if err := newNode(&ExecConfig{Timer: -1}, false).Validate(); err == nil {
		t.Fatal("negative timer should fail")
	}
}
