package httprequest

// Pack-local unit tests for the httpRequest node — URL validation,
// method/body handling, JSON parsing, headers and the outbound policy
// hook. Uses an httptest loopback server (no external network).

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost plus the optional
// OutboundURLAllowed policy hook.
type fakeHost struct {
	allowOutbound func(string) bool
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
func (h *fakeHost) ExecAllowed() bool                                   { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }
func (h *fakeHost) OutboundURLAllowed(rawURL string) bool {
	if h.allowOutbound == nil {
		return true
	}
	return h.allowOutbound(rawURL)
}

func newNode(cfg *HTTPRequestConfig) *HTTPRequest {
	return NewHTTPRequest(nodestore.NewNode("h1", "httpRequest"), cfg, &fakeHost{})
}

func TestRequestURLValidation(t *testing.T) {
	n := newNode(&HTTPRequestConfig{URL: "ftp://x"})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("non-http scheme should be rejected")
	}
	n2 := newNode(&HTTPRequestConfig{})
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("missing URL should error")
	}
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{"url": ""})); err == nil {
		t.Fatal("empty msg url should error")
	}
}

func TestRequestOutboundPolicy(t *testing.T) {
	host := &fakeHost{allowOutbound: func(u string) bool { return false }}
	n := NewHTTPRequest(nodestore.NewNode("h2", "httpRequest"), &HTTPRequestConfig{URL: "http://127.0.0.1/"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("policy-denied URL should error")
	}
}

func TestRequestGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method wrong: %s", r.Method)
		}
		if r.Header.Get("X-Custom") != "v" {
			t.Errorf("custom header missing")
		}
		_, _ = w.Write([]byte("plain-body"))
	}))
	defer srv.Close()

	n := newNode(&HTTPRequestConfig{URL: srv.URL, Headers: map[string]string{"X-Custom": "v"}})
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	resp := out.GetPayload().(map[string]interface{})
	if resp["statusCode"] != 200 || resp["body"] != "plain-body" {
		t.Fatalf("response wrong: %v", resp)
	}
}

func TestRequestJSONBody(t *testing.T) {
	var gotBody map[string]interface{}
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	n := newNode(&HTTPRequestConfig{URL: srv.URL, Method: "POST"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{"k": 1}))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if gotCT != "application/json" {
		t.Fatalf("json content-type missing: %q", gotCT)
	}
	if gotBody["k"] != 1.0 {
		t.Fatalf("body wrong: %v", gotBody)
	}
	resp := out.GetPayload().(map[string]interface{})
	body, ok := resp["body"].(map[string]interface{})
	if !ok || body["ok"] != true {
		t.Fatalf("json response should parse: %v", resp["body"])
	}
}

func TestRequestBasicAuth(t *testing.T) {
	var user, pass string
	var ok bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok = r.BasicAuth()
		_, _ = w.Write([]byte("x"))
	}))
	defer srv.Close()
	n := newNode(&HTTPRequestConfig{URL: srv.URL, Username: "u", Password: "p"})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if !ok || user != "u" || pass != "p" {
		t.Fatal("basic auth not sent")
	}
	// explicit Authorization header wins over config credentials
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _, _ = r.BasicAuth()
		_, _ = w.Write([]byte("x"))
	}))
	defer srv2.Close()
	n2 := newNode(&HTTPRequestConfig{URL: srv2.URL, Username: "u", Password: "p",
		Headers: map[string]string{"Authorization": "Bearer tok"}})
	_, _ = n2.Execute(context.Background(), nodestore.NewMessage())
	if user != "" {
		t.Fatal("explicit Authorization should win")
	}
}

func TestRequestMsgURLOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("from-msg"))
	}))
	defer srv.Close()
	n := newNode(&HTTPRequestConfig{})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{
		"url":    srv.URL,
		"method": "GET",
	}))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["body"] != "from-msg" {
		t.Fatal("msg.url should drive the request")
	}
}

func TestRequestValidateDefaults(t *testing.T) {
	n := newNode(&HTTPRequestConfig{})
	if err := n.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
	if n.config.Method != http.MethodGet {
		t.Fatal("validate should default method to GET")
	}
}
