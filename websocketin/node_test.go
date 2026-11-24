package websocketin

// Pack-local unit tests for the websocketIn node — loopback upgrade,
// message emission with sessionId metadata, session registration for
// websocketOut, and origin allowlist enforcement.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost captures emitted payloads and keeps node state.
type fakeHost struct {
	mu      sync.Mutex // guards emitted/state — connections emit from HTTP handler goroutines
	emitted []nodestore.MessageInterface
	state   map[string]interface{}
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
	return nil
}

func (h *fakeHost) emittedMsgs() []nodestore.MessageInterface {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nodestore.MessageInterface(nil), h.emitted...)
}

func (h *fakeHost) nodeState(key string) interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state[key]
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
	key := wf + ":" + nodeID
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state == nil {
		h.state = map[string]interface{}{}
	}
	if v, ok := h.state[key]; ok {
		return v
	}
	v := f()
	h.state[key] = v
	return v
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

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe failed: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(15 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestWebSocketInRoundTrip(t *testing.T) {
	port := freePort(t)
	host := &fakeHost{}
	n := NewWebSocketInNode(nodestore.NewNode("w1", "websocketIn").SetWorkflowID("wf"),
		&WebSocketInConfig{Path: "/ws", Port: port}, host)
	defer n.Stop()

	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["status"] != "listening" {
		t.Fatalf("status wrong: %v", out.GetPayload())
	}

	conn, _, err := websocket.Dial(context.Background(), fmt.Sprintf("ws://127.0.0.1:%d/ws", port), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")

	if err := conn.Write(context.Background(), websocket.MessageText, []byte("hello")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "ws emission")
	m := host.emittedMsgs()[0]
	if m.GetPayload() != "hello" {
		t.Fatalf("payload wrong: %v", m.GetPayload())
	}
	sid, _ := m.GetMetadata()["sessionId"].(string)
	if sid == "" {
		t.Fatal("sessionId metadata missing")
	}
	// session registered so websocketOut can write back
	sessions := host.nodeState("wf:" + nodebase.WSSessionsKey).(*nodebase.WSSessions)
	if _, ok := sessions.Get(sid); !ok {
		t.Fatalf("session %q not registered", sid)
	}
}

func TestWebSocketInOriginAllowlist(t *testing.T) {
	host := &fakeHost{}
	n := NewWebSocketInNode(nodestore.NewNode("w1", "websocketIn").SetWorkflowID("wf"),
		&WebSocketInConfig{AllowedOrigins: "good.example"}, host)
	opts := n.acceptOptions()
	if len(opts.OriginPatterns) != 1 || opts.OriginPatterns[0] != "good.example" {
		t.Fatalf("allowlist wrong: %+v", opts)
	}
	n2 := NewWebSocketInNode(nodestore.NewNode("w1", "websocketIn").SetWorkflowID("wf"),
		&WebSocketInConfig{}, host)
	if !n2.acceptOptions().InsecureSkipVerify {
		t.Fatal("empty allowlist should keep legacy any-origin mode")
	}
}

func TestWebSocketInValidate(t *testing.T) {
	host := &fakeHost{}
	if err := NewWebSocketInNode(nodestore.NewNode("w1", "websocketIn"),
		&WebSocketInConfig{Port: 9000}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := NewWebSocketInNode(nodestore.NewNode("w1", "websocketIn"),
		&WebSocketInConfig{Port: -1}, host).Validate(); err == nil {
		t.Fatal("negative port should fail")
	}
}

func TestWSSessions(t *testing.T) {
	s := nodebase.NewWSSessions()
	if s.All() == nil || len(s.All()) != 0 {
		t.Fatal("empty registry should have no sessions")
	}
	if _, ok := s.Get("missing"); ok {
		t.Fatal("missing session lookup should fail")
	}
}
