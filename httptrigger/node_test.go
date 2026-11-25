package httptrigger

// Pack-local unit tests for the httpTrigger node — the request
// handler (method gate, body decode, param extraction, response
// correlation, 504 fallback) via httptest and a fake host.

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost returns a scripted injection channel.
type fakeHost struct {
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
	return "req-1", h.responseCh, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error { return nil }
func (h *fakeHost) DiscardPending(id string)                                      { h.discarded = id }
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

func newNode(cfg *HTTPTriggerConfig, host *fakeHost) *HTTPTrigger {
	return NewHTTPTrigger(nodestore.NewNode("h1", "httpTrigger").SetWorkflowID("wf"), cfg, host)
}

func TestTriggerRoundTrip(t *testing.T) {
	host := &fakeHost{responseCh: make(chan nodestore.MessageInterface, 1)}
	n := newNode(&HTTPTriggerConfig{Method: http.MethodPost, Path: "/go/:id"}, host)
	n.params = []string{"id"}

	resp := nodestore.NewMessage().SetPayload("done").SetMetadata(map[string]interface{}{
		"statusCode":      201,
		"responseHeaders": map[string]string{"X-Custom": "v"},
	})
	host.responseCh <- resp

	req := httptest.NewRequest(http.MethodPost, "/go/7?q=1", bytes.NewReader([]byte(`{"k":2}`)))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "7")
	rec := httptest.NewRecorder()
	n.handleRequest(rec, req)

	if rec.Code != 201 {
		t.Fatalf("status wrong: %d", rec.Code)
	}
	if rec.Header().Get("X-Custom") != "v" {
		t.Fatal("response header not propagated")
	}
	if rec.Body.String() != "done" {
		t.Fatalf("body wrong: %q", rec.Body.String())
	}

	// injected payload carries the request object + params
	data := host.injected.GetPayload().(map[string]interface{})
	if data["method"] != http.MethodPost || data["params"].(map[string]string)["id"] != "7" {
		t.Fatalf("injected data wrong: %v", data)
	}
	if data["body"].(map[string]interface{})["k"] != float64(2) {
		t.Fatalf("body decode wrong: %v", data["body"])
	}
	if host.injected.GetMetadata()["path"] != "/go/7" {
		t.Fatal("request metadata missing")
	}
}

func TestTriggerMethodGate(t *testing.T) {
	host := &fakeHost{responseCh: make(chan nodestore.MessageInterface, 1)}
	n := newNode(&HTTPTriggerConfig{Method: http.MethodPost, Path: "/x"}, host)
	rec := httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method should be 405: %d", rec.Code)
	}
}

func TestTriggerBadBody(t *testing.T) {
	host := &fakeHost{responseCh: make(chan nodestore.MessageInterface, 1)}
	n := newNode(&HTTPTriggerConfig{Method: http.MethodPost, Path: "/x"}, host)
	rec := httptest.NewRecorder()
	badReq := httptest.NewRequest(http.MethodPost, "/x", bytes.NewReader([]byte("{bad json")))
	badReq.Header.Set("Content-Type", "application/json")
	n.handleRequest(rec, badReq)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json should be 400: %d", rec.Code)
	}
}

func TestTriggerResponseTimeout(t *testing.T) {
	host := &fakeHost{responseCh: make(chan nodestore.MessageInterface)} // never answers
	n := newNode(&HTTPTriggerConfig{Method: http.MethodGet, Path: "/x", ResponseTimeout: 0.02}, host)
	rec := httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("missing response should be 504: %d", rec.Code)
	}
	if host.discarded != "req-1" {
		t.Fatal("pending not discarded on timeout")
	}
}

func TestTriggerValidate(t *testing.T) {
	host := &fakeHost{}
	n := newNode(&HTTPTriggerConfig{Method: http.MethodGet, Path: "/x"}, host)
	if err := n.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	// empty path fails
	if err := newNode(&HTTPTriggerConfig{Method: http.MethodGet}, host).Validate(); err == nil {
		t.Fatal("missing path should fail")
	}
	if err := newNode(&HTTPTriggerConfig{Method: http.MethodGet, Path: "/x", Listener: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad listener should fail")
	}
	// duplicate path params produce an invalid mux pattern
	if err := newNode(&HTTPTriggerConfig{Method: http.MethodGet, Path: "/u/:id/:id"}, host).Validate(); err == nil {
		t.Fatal("duplicate params should fail")
	}
	// empty method defaults to GET
	m := newNode(&HTTPTriggerConfig{Path: "/x"}, host)
	if err := m.Validate(); err != nil || m.config.Method != "GET" {
		t.Fatal("method should default to GET")
	}
}

func TestTriggerRoutePattern(t *testing.T) {
	n := newNode(&HTTPTriggerConfig{Path: "u/:id"}, &fakeHost{})
	pat, params := n.routePattern()
	if pat != "/u/{id}" || params[0] != "id" {
		t.Fatalf("pattern wrong: %q %v", pat, params)
	}
}

func TestTriggerStopIdempotent(t *testing.T) {
	n := newNode(&HTTPTriggerConfig{}, &fakeHost{})
	if err := n.Stop(); err != nil {
		t.Fatalf("stop without server should be nil: %v", err)
	}
}

func TestTriggerBodyParsing(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		body        string
		want        interface{}
	}{
		{"json", "application/json", `{"k":2}`, map[string]interface{}{"k": float64(2)}},
		{"json vendor suffix", "application/vnd.api+json", `[1,2]`, []interface{}{float64(1), float64(2)}},
		{"form", "application/x-www-form-urlencoded", "a=1&b=x&b=y", map[string]interface{}{"a": "1", "b": []string{"x", "y"}}},
		{"text", "text/plain; charset=utf-8", "hello", "hello"},
		{"binary", "application/octet-stream", "\x01\x02", []byte{1, 2}},
		{"no content type", "", "raw", []byte("raw")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := &fakeHost{responseCh: make(chan nodestore.MessageInterface, 1)}
			n := newNode(&HTTPTriggerConfig{Method: http.MethodPost, Path: "/x"}, host)
			host.responseCh <- nodestore.NewMessage().SetPayload("ok")

			req := httptest.NewRequest(http.MethodPost, "/x", bytes.NewReader([]byte(tc.body)))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()
			n.handleRequest(rec, req)

			if rec.Code != 200 {
				t.Fatalf("status wrong: %d", rec.Code)
			}
			got := host.injected.GetPayload().(map[string]interface{})["body"]
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("body wrong: %#v want %#v", got, tc.want)
			}
		})
	}
}

func TestTriggerMethodsPutOptions(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodOptions, http.MethodPatch, http.MethodHead, http.MethodDelete} {
		host := &fakeHost{responseCh: make(chan nodestore.MessageInterface, 1)}
		n := newNode(&HTTPTriggerConfig{Method: method, Path: "/x"}, host)
		host.responseCh <- nodestore.NewMessage().SetPayload("ok")
		rec := httptest.NewRecorder()
		n.handleRequest(rec, httptest.NewRequest(method, "/x", nil))
		if rec.Code != 200 {
			t.Fatalf("%s rejected: %d", method, rec.Code)
		}
	}
}

func TestTriggerLowercaseMethod(t *testing.T) {
	n := newNode(&HTTPTriggerConfig{Method: "post", Path: "/x"}, &fakeHost{})
	if err := n.Validate(); err != nil {
		t.Fatalf("lowercase method rejected: %v", err)
	}
	if n.config.Method != http.MethodPost {
		t.Fatalf("method not normalized: %q", n.config.Method)
	}
}

func TestTriggerInvalidMethod(t *testing.T) {
	n := newNode(&HTTPTriggerConfig{Method: "FROBNICATE", Path: "/x"}, &fakeHost{})
	if err := n.Validate(); err == nil {
		t.Fatal("bogus method should fail validation")
	}
}

func TestTriggerReqMetadata(t *testing.T) {
	host := &fakeHost{responseCh: make(chan nodestore.MessageInterface, 1)}
	n := newNode(&HTTPTriggerConfig{Method: http.MethodGet, Path: "/x"}, host)
	host.responseCh <- nodestore.NewMessage().SetPayload("ok")

	req := httptest.NewRequest(http.MethodGet, "/x?a=1", nil)
	req.Header.Set("X-Trace", "abc")
	req.AddCookie(&http.Cookie{Name: "sid", Value: "s3cret"})
	rec := httptest.NewRecorder()
	n.handleRequest(rec, req)

	reqMeta, ok := host.injected.GetMetadata()["req"].(map[string]interface{})
	if !ok {
		t.Fatalf("req metadata missing: %v", host.injected.GetMetadata())
	}
	if reqMeta["headers"].(http.Header).Get("X-Trace") != "abc" {
		t.Fatalf("headers not surfaced: %v", reqMeta["headers"])
	}
	if reqMeta["cookies"].(map[string]string)["sid"] != "s3cret" {
		t.Fatalf("cookies not surfaced: %v", reqMeta["cookies"])
	}
	if reqMeta["method"] != http.MethodGet || reqMeta["path"] != "/x" {
		t.Fatalf("req fields wrong: %v", reqMeta)
	}
}

func TestTriggerSwaggerCorsTolerated(t *testing.T) {
	node := nodestore.NewNode("h1", "httpTrigger").SetWorkflowID("wf")
	node.GetData()["method"] = "put"
	node.GetData()["path"] = "/x"
	node.GetData()["swaggerDoc"] = map[string]interface{}{"openapi": "3.0.0"}
	node.GetData()["cors"] = true
	if _, err := factory(node, &fakeHost{}); err != nil {
		t.Fatalf("Node-RED export fields rejected: %v", err)
	}
}

func TestTriggerJSONResponse(t *testing.T) {
	host := &fakeHost{responseCh: make(chan nodestore.MessageInterface, 1)}
	n := newNode(&HTTPTriggerConfig{Method: http.MethodGet, Path: "/x"}, host)
	host.responseCh <- nodestore.NewMessage().SetPayload(map[string]interface{}{"ok": true})
	rec := httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	var body map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil || body["ok"] != true {
		t.Fatalf("json response wrong: %v", rec.Body.String())
	}
	_ = time.Now()
}
