package airequest

// Pack-local unit tests for the ai-request node — mustache rendering,
// parse modes, target write-back, error paths, the concurrency
// semaphore and the custom provider against an httptest stub. No live
// HTTP beyond the local stub; everything else runs on the Mock
// provider with Calls() introspection via the newClient hook.

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	"github.com/dracory/nodebase/aiprovider"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost with a LookupNode table, a
// state slot for the semaphore and an event capture.
type fakeHost struct {
	nodes  map[string]nodestore.NodeInterface
	states map[string]interface{}
	mu     sync.Mutex
	events []string
}

func newFakeHost() *fakeHost {
	return &fakeHost{nodes: map[string]nodestore.NodeInterface{}, states: map[string]interface{}{}}
}

func (h *fakeHost) addNode(n nodestore.NodeInterface) { h.nodes[n.GetID()] = n }

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, level+": "+message)
}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error { return nil }
func (h *fakeHost) DiscardPending(id string)                                      {}
func (h *fakeHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) {
	n, ok := h.nodes[nodeID]
	return n, ok
}
func (h *fakeHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) { return nil, false }
func (h *fakeHost) RunContext(wf string) context.Context                      { return context.Background() }
func (h *fakeHost) GetOrCreateNodeState(wf, nodeID string, f func() interface{}) interface{} {
	key := wf + "/" + nodeID
	if s, ok := h.states[key]; ok {
		return s
	}
	s := f()
	h.states[key] = s
	return s
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

// providerNode builds a stored ai-provider config node for LookupNode.
func providerNode(id string, data map[string]any) nodestore.NodeInterface {
	d := map[string]any{"provider": "mock"}
	for k, v := range data {
		d[k] = v
	}
	return nodestore.NewNode(id, aiprovider.Type).SetData(d)
}

// capture installs a newClient hook returning the real client while
// recording it — so tests can introspect MockInterface.Calls().
func capture(t *testing.T) func() []llm.MockCall {
	t.Helper()
	var mu sync.Mutex
	var last llm.LlmInterface
	orig := newClient
	newClient = func(o llm.LlmOptions) (llm.LlmInterface, error) {
		c, err := llm.NewLLM(o)
		mu.Lock()
		last = c
		mu.Unlock()
		return c, err
	}
	t.Cleanup(func() { newClient = orig })
	return func() []llm.MockCall {
		mu.Lock()
		defer mu.Unlock()
		m, ok := last.(llm.MockInterface)
		if !ok || m == nil {
			return nil
		}
		return m.Calls()
	}
}

func newRequestNode(cfg *Config, host *fakeHost) *RequestNode {
	n := nodestore.NewNode("r1", "ai-request")
	return NewRequestNode(n, cfg, host)
}

func TestTextResponseToPayload(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov", map[string]any{
		"providerOptions": map[string]any{"mock_response": "the answer"},
	}))
	n := newRequestNode(&Config{Provider: "prov", Prompt: "ping"}, host)
	msg := nodestore.NewMessage().SetPayload("x")
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := msg.GetPayload(); got != "the answer" {
		t.Fatalf("payload wrong: %v", got)
	}
}

func TestMustachePromptAndCalls(t *testing.T) {
	calls := capture(t)
	host := newFakeHost()
	host.addNode(providerNode("prov", map[string]any{
		"model":           "mock-model",
		"providerOptions": map[string]any{"mock_response": "ok"},
	}))
	temp := 0.2
	n := newRequestNode(&Config{
		Provider:     "prov",
		SystemPrompt: "be brief",
		Prompt:       "Hello {{payload.name}} ({{topic}})",
		Temperature:  &temp,
		MaxTokens:    64,
	}, host)
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"name": "bob"})
	msg.GetMetadata()["topic"] = "greeting"
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	got := calls()
	if len(got) != 1 {
		t.Fatalf("expected 1 call, got %+v", got)
	}
	if got[0].SystemPrompt != "be brief" || got[0].UserPrompt != "Hello bob (greeting)" {
		t.Fatalf("prompts wrong: %+v", got[0])
	}
	if got[0].Options.Temperature == nil || *got[0].Options.Temperature != 0.2 {
		t.Fatalf("temperature override missing: %+v", got[0].Options)
	}
	if got[0].Options.MaxTokens != 64 {
		t.Fatalf("maxTokens override missing: %+v", got[0].Options)
	}
}

func TestJSONParseMode(t *testing.T) {
	calls := capture(t)
	host := newFakeHost()
	host.addNode(providerNode("prov", map[string]any{
		"providerOptions": map[string]any{"mock_response": `{"a": 1}`},
	}))
	n := newRequestNode(&Config{Provider: "prov", Prompt: "p", ParseMode: "json"}, host)
	msg := nodestore.NewMessage()
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := msg.GetPayload().(map[string]interface{})
	if !ok || m["a"] != float64(1) {
		t.Fatalf("decoded payload wrong: %v", msg.GetPayload())
	}
	got := calls()
	if len(got) != 1 || got[0].Options.OutputFormat != llm.OutputFormatJSON {
		t.Fatalf("json mode must call GenerateJSON: %+v", got)
	}
}

func TestJSONParseModeMalformed(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov", map[string]any{
		"providerOptions": map[string]any{"mock_response": `not json`},
	}))
	n := newRequestNode(&Config{Provider: "prov", Prompt: "p", ParseMode: "json"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("expected malformed-JSON error on the error path")
	}
}

func TestYAMLParseMode(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov", map[string]any{
		"providerOptions": map[string]any{"mock_response": "a: 1\n"},
	}))
	n := newRequestNode(&Config{Provider: "prov", Prompt: "p", ParseMode: "yaml"}, host)
	msg := nodestore.NewMessage()
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := msg.GetPayload().(map[string]interface{})
	if !ok || m["a"] != 1 {
		t.Fatalf("yaml payload wrong: %v", msg.GetPayload())
	}
}

func TestTargetMetadata(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov", map[string]any{
		"providerOptions": map[string]any{"mock_response": "stored"},
	}))
	n := newRequestNode(&Config{Provider: "prov", Prompt: "p", Target: "msg.llm.answer"}, host)
	msg := nodestore.NewMessage().SetPayload("untouched")
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg.GetPayload() != "untouched" {
		t.Fatal("payload must be untouched for msg.* targets")
	}
	v, ok := nodebase.GetPath(msg.GetMetadata(), "llm.answer")
	if !ok || v != "stored" {
		t.Fatalf("metadata target wrong: %v", msg.GetMetadata())
	}
}

func TestMockErrorGoesToErrorPath(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov", map[string]any{
		"providerOptions": map[string]any{"mock_error": "upstream down"},
	}))
	n := newRequestNode(&Config{Provider: "prov", Prompt: "p"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("expected provider error")
	}
}

func TestProviderNotFound(t *testing.T) {
	host := newFakeHost()
	n := newRequestNode(&Config{Provider: "missing", Prompt: "p"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("expected missing-provider error")
	}
	host.addNode(nodestore.NewNode("other", "debug"))
	n2 := newRequestNode(&Config{Provider: "other", Prompt: "p"}, host)
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("expected non-ai-provider reference error")
	}
}

// TestCustomProviderBaseURL exercises the custom provider end to end
// against a local OpenAI-compatible stub — no live HTTP beyond
// httptest.
func TestCustomProviderBaseURL(t *testing.T) {
	var sawAuth, sawModel string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		sawModel = body.Model
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"from stub"}}]}`))
	}))
	defer stub.Close()

	host := newFakeHost()
	host.addNode(providerNode("prov", map[string]any{
		"provider": "custom",
		"baseURL":  stub.URL,
		"apiKey":   "test-key",
		"model":    "test-model",
	}))
	n := newRequestNode(&Config{Provider: "prov", Prompt: "hello"}, host)
	msg := nodestore.NewMessage()
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg.GetPayload() != "from stub" {
		t.Fatalf("payload wrong: %v", msg.GetPayload())
	}
	if sawAuth != "Bearer test-key" || sawModel != "test-model" {
		t.Fatalf("stub saw wrong request: auth=%q model=%q", sawAuth, sawModel)
	}
}

// blockingClient is a minimal LlmInterface whose GenerateText blocks
// until released — used to prove the concurrency semaphore caps
// in-flight calls.
type blockingClient struct {
	inFlight int32
	maxSeen  int32
	release  chan struct{}
}

func (b *blockingClient) GenerateText(s, u string, o ...llm.LlmOptions) (string, error) {
	cur := atomic.AddInt32(&b.inFlight, 1)
	for {
		old := atomic.LoadInt32(&b.maxSeen)
		if cur <= old || atomic.CompareAndSwapInt32(&b.maxSeen, old, cur) {
			break
		}
	}
	<-b.release
	atomic.AddInt32(&b.inFlight, -1)
	return "done", nil
}
func (b *blockingClient) GenerateJSON(s, u string, o ...llm.LlmOptions) (string, error) {
	return b.GenerateText(s, u, o...)
}
func (b *blockingClient) Generate(s, u string, o ...llm.LlmOptions) (string, error) {
	return b.GenerateText(s, u, o...)
}
func (b *blockingClient) GenerateImage(p string, o ...llm.LlmOptions) ([]byte, error) {
	return nil, errors.New("unused")
}
func (b *blockingClient) GenerateEmbedding(t string) ([]float32, error) {
	return nil, errors.New("unused")
}
func (b *blockingClient) Decide(s map[string]any, q map[string]llm.Question, o ...llm.LlmOptions) (map[string]llm.Answer, error) {
	return nil, errors.New("unused")
}

func TestConcurrencySemaphore(t *testing.T) {
	bc := &blockingClient{release: make(chan struct{})}
	orig := newClient
	newClient = func(o llm.LlmOptions) (llm.LlmInterface, error) { return bc, nil }
	t.Cleanup(func() { newClient = orig })

	host := newFakeHost()
	host.addNode(providerNode("prov", nil))
	n := newRequestNode(&Config{Provider: "prov", Prompt: "p", Concurrency: 2}, host)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = n.Execute(context.Background(), nodestore.NewMessage())
		}()
	}
	// Let all goroutines reach the semaphore, then release calls.
	time.Sleep(150 * time.Millisecond)
	close(bc.release)
	wg.Wait()
	if got := atomic.LoadInt32(&bc.maxSeen); got > 2 {
		t.Fatalf("semaphore breached: max in-flight %d > 2", got)
	}
	if got := atomic.LoadInt32(&bc.maxSeen); got < 1 {
		t.Fatal("no calls ran")
	}
}

func TestValidate(t *testing.T) {
	n := newRequestNode(&Config{Prompt: "p"}, nil)
	if err := n.Validate(); err == nil {
		t.Fatal("expected missing-provider error")
	}
	n2 := newRequestNode(&Config{Provider: "p", ParseMode: "xmlish"}, nil)
	if err := n2.Validate(); err == nil {
		t.Fatal("expected bad-parse-mode error")
	}
	n3 := newRequestNode(&Config{Provider: "p", ParseMode: "json"}, nil)
	if err := n3.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
