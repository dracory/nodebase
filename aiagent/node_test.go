package aiagent

// Pack-local unit tests for the ai-agent node: the model ↔ tool loop
// on a scripted in-process client (no HTTP at all), max-iterations
// enforcement, the tool round-trip through InjectMessage/
// ResolveResponse correlation, memory persistence across messages and
// an engine restart (rebuilt node over the same context store), and
// validation.

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	"github.com/dracory/nodebase/aimemory"
	"github.com/dracory/nodebase/aiprovider"
	"github.com/dracory/nodebase/aitool"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost with a LookupNode table, an
// InjectMessage/ResolveResponse pending-correlation table (stamping
// "msgId" like the real engine), an Emit capture and a shareable
// context store.
type fakeHost struct {
	nodes   map[string]nodestore.NodeInterface
	states  map[string]interface{}
	store   nodebase.ContextStore
	mu      sync.Mutex
	seq     int
	pending map[string]chan nodestore.MessageInterface
	emitted []emittedMsg
	events  []string
	// inject is invoked per InjectMessage call so tests can run the
	// tool flow synchronously.
	onInject func(wf, from string, p nodestore.MessageInterface)
}

type emittedMsg struct {
	nodeID string
	ports  []string
	msg    nodestore.MessageInterface
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		nodes:   map[string]nodestore.NodeInterface{},
		states:  map[string]interface{}{},
		pending: map[string]chan nodestore.MessageInterface{},
		store:   nodebase.NewMemoryContextStore(),
	}
}

func (h *fakeHost) addNode(n nodestore.NodeInterface) { h.nodes[n.GetID()] = n }

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, emittedMsg{nodeID: nodeID, ports: ports, msg: p})
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, level+": "+message)
}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	h.mu.Lock()
	h.seq++
	msgID := "fake-msg-" + string(rune('0'+h.seq))
	ch := make(chan nodestore.MessageInterface, 1)
	h.pending[msgID] = ch
	h.mu.Unlock()
	if p.GetMetadata() == nil {
		p.SetMetadata(map[string]interface{}{})
	}
	p.GetMetadata()["msgId"] = msgID
	if h.onInject != nil {
		go h.onInject(wf, from, p)
	}
	return msgID, ch, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error {
	h.mu.Lock()
	ch, ok := h.pending[id]
	delete(h.pending, id)
	h.mu.Unlock()
	if !ok {
		return errors.New("no pending request for message " + id)
	}
	ch <- p
	return nil
}
func (h *fakeHost) DiscardPending(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.pending, id)
}
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
func (h *fakeHost) ResolvedContextStore() nodebase.ContextStore         { return h.store }
func (h *fakeHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)   { return nil, nil }
func (h *fakeHost) FileRootPath() string                                { return "" }
func (h *fakeHost) ExecAllowed() bool                                   { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

// scriptedClient is an in-process llm.ToolsInterface whose queued
// responses are consumed in call order — the deterministic agent-loop
// driver (no HTTP, no mock provider needed).
type scriptedClient struct {
	mu        sync.Mutex
	responses []llm.ToolResponse
	calls     [][]llm.Message // opts.Messages snapshot per call
	prompts   []string
}

func (s *scriptedClient) GenerateWithTools(system, user string, opts ...llm.LlmOptions) (llm.ToolResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var o llm.LlmOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	s.calls = append(s.calls, append([]llm.Message{}, o.Messages...))
	s.prompts = append(s.prompts, user)
	if len(s.responses) == 0 {
		return llm.ToolResponse{}, errors.New("scriptedClient: responses exhausted")
	}
	r := s.responses[0]
	s.responses = s.responses[1:]
	return r, nil
}
func (s *scriptedClient) Generate(system, user string, o ...llm.LlmOptions) (string, error) {
	return "", errors.New("unused")
}
func (s *scriptedClient) GenerateText(system, user string, o ...llm.LlmOptions) (string, error) {
	return "", errors.New("unused")
}
func (s *scriptedClient) GenerateJSON(system, user string, o ...llm.LlmOptions) (string, error) {
	return "", errors.New("unused")
}
func (s *scriptedClient) GenerateImage(p string, o ...llm.LlmOptions) ([]byte, error) {
	return nil, errors.New("unused")
}
func (s *scriptedClient) GenerateEmbedding(t string) ([]float32, error) {
	return nil, errors.New("unused")
}
func (s *scriptedClient) Decide(st map[string]any, q map[string]llm.Question, o ...llm.LlmOptions) (map[string]llm.Answer, error) {
	return nil, errors.New("unused")
}

// script installs a newClient hook returning the scripted client.
func script(t *testing.T, c *scriptedClient) {
	t.Helper()
	orig := newClient
	newClient = func(o llm.LlmOptions) (llm.LlmInterface, error) { return c, nil }
	t.Cleanup(func() { newClient = orig })
}

func providerNode(id string) nodestore.NodeInterface {
	return nodestore.NewNode(id, aiprovider.Type).SetData(map[string]any{"provider": "mock"})
}

func toolNode(id, name string) nodestore.NodeInterface {
	return nodestore.NewNode(id, aitool.Type).SetData(map[string]any{
		"toolName":    name,
		"description": "test tool " + name,
	})
}

func newAgentNode(cfg *Config, host *fakeHost) *AgentNode {
	n := nodestore.NewNode("agent1", Type)
	return NewAgentNode(n, cfg, host)
}

// toolCallResponse builds an assistant turn requesting one tool call.
func toolCallResponse(id, name, args string) llm.ToolResponse {
	calls := []llm.ToolCall{{ID: id, Type: "function", Name: name, Arguments: args}}
	return llm.ToolResponse{
		ToolCalls: calls,
		Message:   llm.Message{Role: llm.RoleAssistant, ToolCalls: calls},
	}
}

func answerResponse(text string) llm.ToolResponse {
	return llm.ToolResponse{
		Content: text,
		Message: llm.Message{Role: llm.RoleAssistant, Content: text},
	}
}

func TestAgentLoopToolCallThenAnswer(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov"))
	host.addNode(toolNode("weather", "get_weather"))

	// The tool flow: ai-tool stamps metadata, ai-tool-return resolves.
	host.onInject = func(wf, from string, p nodestore.MessageInterface) {
		if p.GetPayload().(map[string]interface{})["city"] != "Paris" {
			t.Errorf("tool args wrong: %v", p.GetPayload())
		}
		p.SetPayload("sunny, 22C")
		if err := host.ResolveResponse(p.GetMetadata()["msgId"].(string), p); err != nil {
			t.Errorf("resolve failed: %v", err)
		}
	}

	sc := &scriptedClient{responses: []llm.ToolResponse{
		toolCallResponse("call-1", "get_weather", `{"city":"Paris"}`),
		answerResponse("It is sunny in Paris."),
	}}
	script(t, sc)

	n := newAgentNode(&Config{
		Provider: "prov", Prompt: "weather?", Tools: "weather",
		ToolTimeoutSeconds: 5,
	}, host)
	msg := nodestore.NewMessage()
	out, err := n.Execute(context.Background(), msg)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "It is sunny in Paris." {
		t.Fatalf("answer wrong: %v", out.GetPayload())
	}
	if len(sc.calls) != 2 {
		t.Fatalf("expected 2 model calls, got %d", len(sc.calls))
	}
	// Second call must carry the assistant turn plus the tool result.
	second := sc.calls[1]
	var sawAssistant, sawTool bool
	for _, m := range second {
		if m.Role == llm.RoleAssistant && len(m.ToolCalls) == 1 {
			sawAssistant = true
		}
		if m.Role == llm.RoleTool && m.ToolCallID == "call-1" && m.Content == "sunny, 22C" {
			sawTool = true
		}
	}
	if !sawAssistant || !sawTool {
		t.Fatalf("second call messages missing turns: %+v", second)
	}
	if sc.prompts[1] != "" {
		t.Fatalf("continuation call should pass an empty user prompt, got %q", sc.prompts[1])
	}
	// Trace emitted on output_2 with call + result + answer entries.
	if len(host.emitted) != 1 || len(host.emitted[0].ports) != 1 || host.emitted[0].ports[0] != "output_2" {
		t.Fatalf("trace emit wrong: %+v", host.emitted)
	}
	trace, _ := host.emitted[0].msg.GetPayload().([]interface{})
	if len(trace) != 3 {
		t.Fatalf("trace entries wrong: %+v", trace)
	}
}

func TestMaxIterationsEnforced(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov"))
	host.addNode(toolNode("weather", "get_weather"))
	host.onInject = func(wf, from string, p nodestore.MessageInterface) {
		p.SetPayload("ok")
		_ = host.ResolveResponse(p.GetMetadata()["msgId"].(string), p)
	}
	sc := &scriptedClient{}
	for i := 0; i < 5; i++ {
		sc.responses = append(sc.responses, toolCallResponse("c", "get_weather", `{}`))
	}
	script(t, sc)

	n := newAgentNode(&Config{
		Provider: "prov", Prompt: "p", Tools: "weather",
		MaxIterations: 3, ToolTimeoutSeconds: 5,
	}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("expected max-iterations error")
	}
	if len(sc.calls) != 3 {
		t.Fatalf("expected exactly 3 model calls, got %d", len(sc.calls))
	}
	// Trace still emitted with the call/result pairs.
	if len(host.emitted) != 1 {
		t.Fatal("trace must be emitted even on the error path")
	}
}

func TestDirectAnswerNoTools(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov"))
	script(t, &scriptedClient{responses: []llm.ToolResponse{answerResponse("42")}})
	n := newAgentNode(&Config{Provider: "prov", Prompt: "p"}, host)
	msg := nodestore.NewMessage()
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if msg.GetPayload() != "42" {
		t.Fatalf("payload wrong: %v", msg.GetPayload())
	}
	if ports := n.SelectPorts(msg, msg); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("answer must route to output_1: %v", ports)
	}
}

func TestMemoryAcrossMessagesAndRestart(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov"))
	host.addNode(nodestore.NewNode("mem", aimemory.Type).SetData(map[string]any{
		"keyProperty": "msg.conversationId",
	}))

	// First run: one exchange for conversation "c1".
	script(t, &scriptedClient{responses: []llm.ToolResponse{answerResponse("hi there")}})
	n := newAgentNode(&Config{Provider: "prov", Prompt: "hello", Memory: "mem"}, host)
	m1 := nodestore.NewMessage()
	m1.GetMetadata()["conversationId"] = "c1"
	if _, err := n.Execute(context.Background(), m1); err != nil {
		t.Fatalf("run 1 failed: %v", err)
	}

	// "Restart": a rebuilt node/host over the SAME context store.
	host2 := newFakeHost()
	host2.store = host.store // the durable backend survives
	host2.addNode(providerNode("prov"))
	host2.addNode(nodestore.NewNode("mem", aimemory.Type).SetData(map[string]any{
		"keyProperty": "msg.conversationId",
	}))
	sc := &scriptedClient{responses: []llm.ToolResponse{answerResponse("again?")}}
	script(t, sc)
	n2 := newAgentNode(&Config{Provider: "prov", Prompt: "follow up", Memory: "mem"}, host2)
	m2 := nodestore.NewMessage()
	m2.GetMetadata()["conversationId"] = "c1"
	if _, err := n2.Execute(context.Background(), m2); err != nil {
		t.Fatalf("run 2 failed: %v", err)
	}
	// The second call's Messages must carry run 1's user+assistant turns.
	var sawUser, sawAssistant bool
	for _, m := range sc.calls[0] {
		if m.Role == llm.RoleUser && m.Content == "hello" {
			sawUser = true
		}
		if m.Role == llm.RoleAssistant && m.Content == "hi there" {
			sawAssistant = true
		}
	}
	if !sawUser || !sawAssistant {
		t.Fatalf("history did not survive restart: %+v", sc.calls[0])
	}

	// A different conversation id sees no history.
	sc2 := &scriptedClient{responses: []llm.ToolResponse{answerResponse("x")}}
	script(t, sc2)
	m3 := nodestore.NewMessage()
	m3.GetMetadata()["conversationId"] = "other"
	n3 := newAgentNode(&Config{Provider: "prov", Prompt: "p", Memory: "mem"}, host2)
	if _, err := n3.Execute(context.Background(), m3); err != nil {
		t.Fatalf("run 3 failed: %v", err)
	}
	if len(sc2.calls[0]) != 0 {
		t.Fatalf("foreign conversation leaked history: %+v", sc2.calls[0])
	}
}

func TestUnknownToolReportedBack(t *testing.T) {
	host := newFakeHost()
	host.addNode(providerNode("prov"))
	sc := &scriptedClient{responses: []llm.ToolResponse{
		toolCallResponse("c1", "nonexistent", `{}`),
		answerResponse("recovered"),
	}}
	script(t, sc)
	n := newAgentNode(&Config{Provider: "prov", Prompt: "p"}, host)
	msg := nodestore.NewMessage()
	if _, err := n.Execute(context.Background(), msg); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	// The tool error went back to the model as a tool result.
	var sawErr bool
	for _, m := range sc.calls[1] {
		if m.Role == llm.RoleTool && m.Content != "" && m.Content[:6] == "error:" {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatalf("unknown-tool error not fed back: %+v", sc.calls[1])
	}
}

func TestValidate(t *testing.T) {
	n := newAgentNode(&Config{Prompt: "p"}, nil)
	if err := n.Validate(); err == nil {
		t.Fatal("expected missing-provider error")
	}
	n2 := newAgentNode(&Config{Provider: "p", MaxIterations: -1}, nil)
	if err := n2.Validate(); err == nil {
		t.Fatal("expected negative-iterations error")
	}
	n3 := newAgentNode(&Config{Provider: "p", MaxIterations: 3}, nil)
	if err := n3.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
