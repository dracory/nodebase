package uichart

// Pack-local unit tests for the uiChart widget — scalar/array push
// semantics and ui overrides via a bound sink.

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

func TestUIChartScalarAndArray(t *testing.T) {
	host := &stubHost{}
	sk := &sink{}
	defer nodebase.BindUIWidgetSink(host, sk)()
	n := NewUIChartNode(nodestore.NewNode("c1", "uiChart").SetWorkflowID("wf"), host, &UIChartConfig{})

	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(3.5))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out == nil {
		t.Fatal("message should pass through")
	}
	if sk.states[0]["value"] != 3.5 {
		t.Fatalf("scalar push wrong: %v", sk.states[0])
	}

	_, _ = n.Execute(context.Background(), nodestore.NewMessage().SetPayload([]interface{}{1, 2}))
	vals, _ := sk.states[1]["values"].([]float64)
	if len(vals) != 2 {
		t.Fatalf("array push wrong: %v", sk.states[1])
	}

	// non-numeric pushes nothing
	before := len(sk.states)
	_, _ = n.Execute(context.Background(), nodestore.NewMessage().SetPayload("not a number"))
	if len(sk.states) != before {
		t.Fatal("non-numeric payload pushed state")
	}
}

func TestUIChartOverride(t *testing.T) {
	host := &stubHost{}
	sk := &sink{}
	defer nodebase.BindUIWidgetSink(host, sk)()
	n := NewUIChartNode(nodestore.NewNode("c1", "uiChart").SetWorkflowID("wf"), host, &UIChartConfig{})
	_, _ = n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{
		"payload": 5, "ui": map[string]interface{}{"value": 9},
	}))
	if sk.states[0]["value"] != 9 {
		t.Fatalf("ui override wrong: %v", sk.states[0])
	}
}
