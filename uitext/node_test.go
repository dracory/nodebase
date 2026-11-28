package uitext

// Pack-local unit tests for the uiText widget — formatted value push
// and label overrides.

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

type stubHost struct{}

func (h *stubHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *stubHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *stubHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *stubHost) ResolveResponse(id string, p nodestore.MessageInterface) error { return nil }
func (h *stubHost) DiscardPending(id string)                                      {}
func (h *stubHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) {
	return nil, false
}
func (h *stubHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) { return nil, false }
func (h *stubHost) RunContext(wf string) context.Context                      { return context.Background() }
func (h *stubHost) GetOrCreateNodeState(wf, nodeID string, f func() interface{}) interface{} {
	return f()
}
func (h *stubHost) ExecuteDownstream(ctx context.Context, wf, from string, w nodestore.WorkflowInterface, p nodestore.MessageInterface, ports []string) {
}
func (h *stubHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) { return nil, false }
func (h *stubHost) ResolvedContextStore() nodebase.ContextStore         { return nil }
func (h *stubHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)   { return nil, nil }
func (h *stubHost) FileRootPath() string                                { return "" }
func (h *stubHost) ExecAllowed() bool                                   { return false }
func (h *stubHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *stubHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *stubHost) QueueService() nodebase.QueueService                 { return nil }

type sink struct{ states []map[string]interface{} }

func (s *sink) UpdateWidget(wf, nodeID string, st map[string]interface{}) {
	s.states = append(s.states, st)
}

func TestUIText(t *testing.T) {
	host := &stubHost{}
	sk := &sink{}
	defer nodebase.BindUIWidgetSink(host, sk)()
	n := NewUITextNode(nodestore.NewNode("t1", "uiText").SetWorkflowID("wf"), host)

	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(42))
	if err != nil || out == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if sk.states[0]["value"] != "42" {
		t.Fatalf("value push wrong: %v", sk.states[0])
	}
	if _, present := sk.states[0]["label"]; present {
		t.Fatal("empty label placeholder should be removed")
	}

	// label override survives
	_, _ = n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{
		"payload": "x", "ui": map[string]interface{}{"label": "Temp"},
	}))
	if sk.states[1]["label"] != "Temp" {
		t.Fatalf("label override wrong: %v", sk.states[1])
	}
}
