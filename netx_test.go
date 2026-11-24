package nodebase

// Pack-local unit tests for the network helpers shared by the
// tcp/udp/http packs and the UI-widget helpers in uishare.go.

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

// stubHost is the minimal NodeHost for root-package seam tests —
// shared by netx_test.go, tls_test.go and queue_test.go.
type stubHost struct {
	env map[string]interface{}
}

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
func (h *stubHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) {
	v, ok := h.env[name]
	return v, ok
}
func (h *stubHost) ResolvedContextStore() ContextStore                 { return nil }
func (h *stubHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)  { return nil, nil }
func (h *stubHost) FileRootPath() string                               { return "" }
func (h *stubHost) ExecAllowed() bool                                  { return false }
func (h *stubHost) SharedHTTPRouter() HTTPRouter                       { return nil }
func (h *stubHost) HTTPAuthFunc() func(r *http.Request) (string, bool) { return nil }
func (h *stubHost) QueueService() QueueService                         { return nil }

func TestTCPPayloadBytes(t *testing.T) {
	if _, err := TCPPayloadBytes(nil); err == nil {
		t.Fatal("nil message should error")
	}
	if _, err := TCPPayloadBytes(nodestore.NewMessage()); err == nil {
		t.Fatal("nil payload should error")
	}
	b, err := TCPPayloadBytes(nodestore.NewMessage().SetPayload("hi"))
	if err != nil || string(b) != "hi" {
		t.Fatalf("string passthrough wrong: %q", b)
	}
	b, _ = TCPPayloadBytes(nodestore.NewMessage().SetPayload([]byte{1, 2}))
	if len(b) != 2 {
		t.Fatal("[]byte passthrough wrong")
	}
	// other types serialize as JSON with a trailing newline
	b, err = TCPPayloadBytes(nodestore.NewMessage().SetPayload(map[string]interface{}{"a": 1}))
	if err != nil || string(b) != "{\"a\":1}\n" {
		t.Fatalf("json+newline wrong: %q", b)
	}
	if _, err := TCPPayloadBytes(nodestore.NewMessage().SetPayload(func() {})); err == nil {
		t.Fatal("unserializable payload should error")
	}
}

func TestUnescapeTCP(t *testing.T) {
	if got := UnescapeTCP(`a\nb\rc\td`); got != "a\nb\rc\td" {
		t.Fatalf("escapes wrong: %q", got)
	}
	if got := UnescapeTCP("plain"); got != "plain" {
		t.Fatalf("plain wrong: %q", got)
	}
}

func TestMuxPattern(t *testing.T) {
	pat, params := MuxPattern("/u/:id/f/:name")
	if pat != "/u/{id}/f/{name}" {
		t.Fatalf("pattern wrong: %q", pat)
	}
	if len(params) != 2 || params[0] != "id" || params[1] != "name" {
		t.Fatalf("params wrong: %v", params)
	}
	pat, params = MuxPattern("/static")
	if pat != "/static" || len(params) != 0 {
		t.Fatalf("static pattern wrong: %q %v", pat, params)
	}
	// bare ":" is not a parameter
	pat, params = MuxPattern("/:")
	if pat != "/:" || len(params) != 0 {
		t.Fatalf("colon-only segment wrong: %q %v", pat, params)
	}
}

func TestUIWidgetSinkBinding(t *testing.T) {
	host := &stubHost{}
	var got map[string]interface{}
	unbind := BindUIWidgetSink(host, sinkFunc(func(wf, nodeID string, state map[string]interface{}) {
		got = state
	}))
	defer unbind()
	PushUIState(host, "wf", "n1", map[string]interface{}{"value": 1})
	if got["value"] != 1 {
		t.Fatalf("state not delivered: %v", got)
	}
	// unbound host is a no-op
	PushUIState(nil, "wf", "n1", map[string]interface{}{"value": 2})
	unbind()
	got = nil
	PushUIState(host, "wf", "n1", map[string]interface{}{"value": 3})
	if got != nil {
		t.Fatal("state delivered after unbind")
	}
}

type sinkFunc func(wf, nodeID string, state map[string]interface{})

func (f sinkFunc) UpdateWidget(wf, nodeID string, state map[string]interface{}) {
	f(wf, nodeID, state)
}

func TestUIValueHelpers(t *testing.T) {
	// UIPayloadValue: Data["payload"] wins
	if v := UIPayloadValue(map[string]interface{}{"payload": "x"}); v != "x" {
		t.Fatalf("payload unwrap wrong: %v", v)
	}
	if v := UIPayloadValue(map[string]interface{}{"other": 1}); v == nil {
		t.Fatal("map without payload should pass through")
	}
	// overrides merge only existing keys
	state := map[string]interface{}{"value": "a", "label": "b"}
	UIApplyOverrides(state, map[string]interface{}{"label": "L", "bogus": "x"})
	if state["label"] != "L" || state["value"] != "a" {
		t.Fatalf("override merge wrong: %v", state)
	}
	if _, present := state["bogus"]; present {
		t.Fatal("override must not invent keys")
	}
	// format
	if UIFormatValue("s") != "s" || UIFormatValue(1.5) != "1.5" || UIFormatValue(nil) != "" {
		t.Fatal("UIFormatValue wrong")
	}
	if UIFormatValue(map[string]interface{}{"a": 1}) != `{"a":1}` {
		t.Fatal("UIFormatValue json wrong")
	}
	// number coercion
	if f, ok := UINumberValue("2.5"); !ok || f != 2.5 {
		t.Fatal("string→number wrong")
	}
	if _, ok := UINumberValue("nope"); ok {
		t.Fatal("non-numeric string should fail")
	}
	if f, ok := UINumberValue(int64(3)); !ok || f != 3 {
		t.Fatal("int64→number wrong")
	}
	// slice coercion
	vals := UINumberSlice([]interface{}{1, "x", 2.5})
	if len(vals) != 2 {
		t.Fatalf("number slice wrong: %v", vals)
	}
	vals = UINumberSlice(7)
	if len(vals) != 1 || vals[0] != 7 {
		t.Fatalf("scalar→slice wrong: %v", vals)
	}
}
