package uinotification

// Pack-local unit tests for the uiNotification widget — text/level
// push including map payload level overrides.

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

func TestUINotification(t *testing.T) {
	host := &stubHost{}
	sk := &sink{}
	defer nodebase.BindUIWidgetSink(host, sk)()
	n := NewUINotificationNode(nodestore.NewNode("n1", "uiNotification").SetWorkflowID("wf"), host)

	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("heads up"))
	if err != nil || out == nil {
		t.Fatalf("execute failed: %v", err)
	}
	if sk.states[0]["text"] != "heads up" || sk.states[0]["level"] != "info" {
		t.Fatalf("notification wrong: %v", sk.states[0])
	}

	// map payload carries text + level
	_, _ = n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{
		"payload": map[string]interface{}{"text": "alarm", "level": "error"},
	}))
	if sk.states[1]["text"] != "alarm" || sk.states[1]["level"] != "error" {
		t.Fatalf("map notification wrong: %v", sk.states[1])
	}
}
