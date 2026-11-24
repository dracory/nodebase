package websocketout

// Pack-local unit tests for the websocketOut node — session and
// broadcast writes through the shared WSSessions registry plus the
// payload framing helper.

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost carries the node-state slot holding WSSessions.
type fakeHost struct {
	state map[string]interface{}
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
	key := wf + ":" + nodeID
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

func newNode(cfg *WebSocketOutConfig, host *fakeHost) *WebSocketOutNode {
	return NewWebSocketOutNode(nodestore.NewNode("o1", "websocketOut").SetWorkflowID("wf"), cfg, host)
}

// wsPair returns a client conn and registers the server conn under
// "s1" in the host session registry.
func wsPair(t *testing.T, host *fakeHost) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		sessions := host.GetOrCreateNodeState("wf", nodebase.WSSessionsKey, func() interface{} {
			return nodebase.NewWSSessions()
		}).(*nodebase.WSSessions)
		sessions.Put("s1", c)
		time.Sleep(1200 * time.Millisecond) // keep the handler alive while the client reads
	}))
	t.Cleanup(srv.Close)
	conn, _, err := websocket.Dial(context.Background(),
		strings.Replace(srv.URL, "http", "ws", 1), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "done") })
	// wait for registration
	deadline := time.Now().Add(2 * time.Second)
	for {
		sessions := host.GetOrCreateNodeState("wf", nodebase.WSSessionsKey, func() interface{} {
			return nodebase.NewWSSessions()
		}).(*nodebase.WSSessions)
		if _, ok := sessions.Get("s1"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("session never registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return conn
}

func TestWSOutSession(t *testing.T) {
	host := &fakeHost{}
	conn := wsPair(t, host)
	n := newNode(&WebSocketOutConfig{Target: "session"}, host)
	in := nodestore.NewMessage().SetPayload("reply").SetMetadata(map[string]interface{}{
		"sessionId": "s1",
	})
	out, err := n.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != in {
		t.Fatal("message should pass through")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil || string(data) != "reply" {
		t.Fatalf("ws read wrong: %q %v", data, err)
	}
}

func TestWSOutBroadcast(t *testing.T) {
	host := &fakeHost{}
	conn := wsPair(t, host)
	n := newNode(&WebSocketOutConfig{Target: "broadcast"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("all")); err != nil {
		t.Fatalf("broadcast failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil || string(data) != "all" {
		t.Fatalf("broadcast read wrong: %q %v", data, err)
	}
}

func TestWSOutErrors(t *testing.T) {
	host := &fakeHost{}
	// no sessions at all
	n := newNode(&WebSocketOutConfig{Target: "broadcast"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("broadcast with no sessions should fail")
	}
	// session mode missing metadata
	n2 := newNode(&WebSocketOutConfig{Target: "session"}, host)
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("missing sessionId should fail")
	}
	// unknown session id
	in := nodestore.NewMessage().SetPayload("x").SetMetadata(map[string]interface{}{"sessionId": "ghost"})
	if _, err := n2.Execute(context.Background(), in); err == nil {
		t.Fatal("unknown session should fail")
	}
	// bad target
	n3 := newNode(&WebSocketOutConfig{Target: "bogus"}, host)
	if _, err := n3.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("bad target should fail")
	}
	// empty payload
	if _, err := n3.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("empty payload should fail")
	}
}

func TestWSOutValidate(t *testing.T) {
	host := &fakeHost{}
	if err := newNode(&WebSocketOutConfig{Target: "session"}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&WebSocketOutConfig{Target: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad target should fail")
	}
}
