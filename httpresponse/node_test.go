package httpresponse

// Pack-local unit tests for the httpResponse node — msgId correlation,
// statusCode/header precedence — against a fake host.

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost captures ResolveResponse calls.
type fakeHost struct {
	resolved   map[string]nodestore.MessageInterface
	resolveErr error
}

func newFakeHost() *fakeHost { return &fakeHost{resolved: map[string]nodestore.MessageInterface{}} }

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error {
	if h.resolveErr != nil {
		return h.resolveErr
	}
	h.resolved[id] = p
	return nil
}
func (h *fakeHost) DiscardPending(id string) {}
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

func newNode(cfg *HTTPResponseConfig, host *fakeHost) *HTTPResponse {
	return NewHTTPResponse(nodestore.NewNode("r1", "httpResponse"), cfg, host)
}

func TestResponseNoMsgID(t *testing.T) {
	host := newFakeHost()
	n := newNode(&HTTPResponseConfig{}, host)
	_, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err == nil {
		t.Fatal("missing msgId should error")
	}
}

func TestResponseDefaults(t *testing.T) {
	host := newFakeHost()
	n := newNode(&HTTPResponseConfig{}, host)
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{"msgId": "m1"})
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if _, ok := host.resolved["m1"]; !ok {
		t.Fatal("response should resolve the pending request")
	}
	if msg.GetMetadata()["statusCode"] != 200 {
		t.Fatalf("default status wrong: %v", msg.GetMetadata()["statusCode"])
	}
}

func TestResponseStatusPrecedence(t *testing.T) {
	host := newFakeHost()
	n := newNode(&HTTPResponseConfig{StatusCode: 201}, host)
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{
		"msgId":      "m1",
		"statusCode": float64(404),
	})
	_, _ = n.Execute(context.Background(), msg)
	if msg.GetMetadata()["statusCode"] != 404 {
		t.Fatalf("metadata statusCode should win: %v", msg.GetMetadata()["statusCode"])
	}

	// payload statusCode beats config when metadata absent
	msg2 := nodestore.NewMessage().SetPayload(map[string]interface{}{"statusCode": float64(503)})
	msg2.SetMetadata(map[string]interface{}{"msgId": "m2"})
	_, _ = n.Execute(context.Background(), msg2)
	if msg2.GetMetadata()["statusCode"] != 503 {
		t.Fatalf("payload statusCode should win over config: %v", msg2.GetMetadata()["statusCode"])
	}
}

func TestResponseHeadersMerge(t *testing.T) {
	host := newFakeHost()
	n := newNode(&HTTPResponseConfig{Headers: map[string]string{"X-A": "1", "X-B": "cfg"}}, host)
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"headers": map[string]interface{}{"X-B": "msg", "X-C": "3"},
	})
	msg.SetMetadata(map[string]interface{}{
		"msgId":   "m1",
		"headers": map[string]interface{}{"X-D": "4"},
	})
	_, _ = n.Execute(context.Background(), msg)
	h := msg.GetMetadata()["responseHeaders"].(map[string]string)
	if h["X-A"] != "1" || h["X-B"] != "msg" || h["X-C"] != "3" || h["X-D"] != "4" {
		t.Fatalf("header merge wrong: %v", h)
	}
}
