package submitin

// Pack-local unit tests for the submitIn node — the HTTP upload
// handler (multipart/raw bodies, auth gate, size cap, route params)
// exercised directly through httptest against a fake host.

import (
	"bytes"
	"context"
	"crypto/tls"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost records injected payloads and serves an auth function.
type fakeHost struct {
	injected  []nodestore.MessageInterface
	injectErr error
	authFunc  func(r *http.Request) (string, bool)
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	if h.injectErr != nil {
		return "", nil, h.injectErr
	}
	h.injected = append(h.injected, p)
	return "msg-1", nil, nil
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
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return h.authFunc }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

func newNode(cfg *SubmitConfig, host *fakeHost) *SubmitNode {
	return NewSubmitNode(nodestore.NewNode("s1", "submitIn").SetWorkflowID("wf"), cfg, host)
}

func multipartBody(t *testing.T, field, filename, content string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile(field, filename)
	if err != nil {
		t.Fatalf("multipart create failed: %v", err)
	}
	_, _ = part.Write([]byte(content))
	_ = w.Close()
	return &buf, w.FormDataContentType()
}

func TestSubmitRawUpload(t *testing.T) {
	host := &fakeHost{}
	n := newNode(&SubmitConfig{Path: "/up"}, host)
	req := httptest.NewRequest(http.MethodPost, "/up?filename=doc.txt", bytes.NewReader([]byte("hello")))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	n.handleRequest(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload rejected: %d %s", rec.Code, rec.Body.String())
	}
	if len(host.injected) != 1 {
		t.Fatal("payload not injected")
	}
	sub := host.injected[0].GetMetadata()["submit"].(map[string]interface{})
	if sub["filename"] != "doc.txt" || sub["field"] != "body" || sub["size"] != int64(5) {
		t.Fatalf("submit metadata wrong: %v", sub)
	}
	if string(host.injected[0].GetPayload().([]byte)) != "hello" {
		t.Fatal("payload bytes wrong")
	}
}

func TestSubmitMultipartUpload(t *testing.T) {
	host := &fakeHost{}
	n := newNode(&SubmitConfig{Path: "/up"}, host)
	body, ct := multipartBody(t, "file", "a.bin", "binary-data")
	req := httptest.NewRequest(http.MethodPost, "/up", body)
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	n.handleRequest(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("multipart rejected: %d %s", rec.Code, rec.Body.String())
	}
	sub := host.injected[0].GetMetadata()["submit"].(map[string]interface{})
	if sub["filename"] != "a.bin" || sub["field"] != "file" {
		t.Fatalf("multipart metadata wrong: %v", sub)
	}
}

func TestSubmitMethodAndSize(t *testing.T) {
	host := &fakeHost{}
	n := newNode(&SubmitConfig{Path: "/up", MaxSize: 4}, host)

	rec := httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodGet, "/up", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET should be 405: %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodPost, "/up", bytes.NewReader([]byte("123456789"))))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized upload should be 413: %d", rec.Code)
	}
}

func TestSubmitAdminAuth(t *testing.T) {
	// no authenticator on host → 503
	host := &fakeHost{}
	n := newNode(&SubmitConfig{Path: "/up", Auth: "admin"}, host)
	rec := httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodPost, "/up", bytes.NewReader([]byte("x"))))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing authenticator should be 503: %d", rec.Code)
	}

	// authenticator rejects → 401
	host.authFunc = func(r *http.Request) (string, bool) { return "", false }
	rec = httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodPost, "/up", bytes.NewReader([]byte("x"))))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("rejected auth should be 401: %d", rec.Code)
	}

	// authenticator accepts → username recorded
	host.authFunc = func(r *http.Request) (string, bool) { return "admin-user", true }
	rec = httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodPost, "/up", bytes.NewReader([]byte("x"))))
	if rec.Code != http.StatusOK {
		t.Fatalf("authenticated upload should pass: %d", rec.Code)
	}
	sub := host.injected[0].GetMetadata()["submit"].(map[string]interface{})
	if sub["submittedBy"] != "admin-user" {
		t.Fatalf("submittedBy wrong: %v", sub)
	}
}

func TestSubmitInjectError(t *testing.T) {
	host := &fakeHost{injectErr: http.ErrAbortHandler}
	n := newNode(&SubmitConfig{Path: "/up"}, host)
	rec := httptest.NewRecorder()
	n.handleRequest(rec, httptest.NewRequest(http.MethodPost, "/up", bytes.NewReader([]byte("x"))))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("inject failure should be 503: %d", rec.Code)
	}
}

func TestListenerSchemaOptionsExplainChoices(t *testing.T) {
	props := schema["properties"].(map[string]interface{})
	listener := props["listener"].(map[string]interface{})
	options, ok := listener["options"].([]map[string]interface{})
	if !ok || len(options) != 2 {
		t.Fatalf("listener schema should expose two rich options, got %#v", listener["options"])
	}
	if options[0]["value"] != "own" || options[0]["description"] == "" {
		t.Fatalf("own option should have an explanation: %#v", options[0])
	}
	if options[1]["value"] != "main" || options[1]["description"] == "" {
		t.Fatalf("main option should have an explanation: %#v", options[1])
	}
}

func TestSubmitValidate(t *testing.T) {
	host := &fakeHost{}
	n := newNode(&SubmitConfig{}, host)
	if err := n.Validate(); err != nil {
		t.Fatalf("default config should pass: %v", err)
	}
	if n.config.Path != "/submit" {
		t.Fatalf("default path wrong: %q", n.config.Path)
	}
	if err := newNode(&SubmitConfig{Auth: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad auth should fail")
	}
	if err := newNode(&SubmitConfig{Listener: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad listener should fail")
	}
	if err := newNode(&SubmitConfig{MaxSize: -1}, host).Validate(); err == nil {
		t.Fatal("negative maxSize should fail")
	}
}

func TestSubmitRouteParams(t *testing.T) {
	n := newNode(&SubmitConfig{Path: "/u/:id"}, &fakeHost{})
	pat, params := n.routePattern()
	if pat != "/u/{id}" || len(params) != 1 || params[0] != "id" {
		t.Fatalf("route pattern wrong: %q %v", pat, params)
	}
}

func TestSubmitStopIdempotent(t *testing.T) {
	n := newNode(&SubmitConfig{}, &fakeHost{})
	if err := n.Stop(); err != nil {
		t.Fatalf("stop without server should be nil: %v", err)
	}
}
