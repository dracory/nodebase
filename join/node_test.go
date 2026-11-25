package join

// Pack-local unit tests for the join node — auto (parts) and custom
// grouping, all build modes, validation and timeout flushing, run
// against a fake host (no engine).

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost implements nodebase.NodeHost with an in-memory state slot
// and an emitted-message capture for the timeout flush path.
type fakeHost struct {
	states  map[string]interface{}
	mu      sync.Mutex // guards emitted — the timeout flush emits from a timer goroutine
	emitted []nodestore.MessageInterface
}

func newFakeHost() *fakeHost { return &fakeHost{states: map[string]interface{}{}} }

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

func newNode(cfg *JoinConfig, host *fakeHost) *JoinNode {
	return NewJoinNode(nodestore.NewNode("j1", "join").SetWorkflowID("wf"), cfg, host)
}

func partMsg(id string, index, count int, payload interface{}) nodestore.MessageInterface {
	m := nodestore.NewMessage().SetPayload(payload)
	m.SetMetadata(map[string]interface{}{
		"parts": map[string]interface{}{"id": id, "index": index, "count": count},
	})
	return m
}

func TestJoinAutoArray(t *testing.T) {
	host := newFakeHost()
	n := newNode(&JoinConfig{}, host)
	ctx := context.Background()

	out, err := n.Execute(ctx, partMsg("g1", 1, 2, "b"))
	if err != nil || out != nil {
		t.Fatal("first part should be buffered")
	}
	out, err = n.Execute(ctx, partMsg("g1", 0, 2, "a"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("expected joined array, got %v", out.GetPayload())
	}
	// ordered by parts.index, not arrival order
	if arr[0] != "a" || arr[1] != "b" {
		t.Fatalf("join order wrong: %v", arr)
	}
}

func TestJoinAutoPassthrough(t *testing.T) {
	host := newFakeHost()
	n := newNode(&JoinConfig{}, host)
	msg := nodestore.NewMessage().SetPayload("x") // no parts
	out, err := n.Execute(context.Background(), msg)
	// Node-RED warns "Message missing msg.parts property" and drops it
	if err != nil || out != nil {
		t.Fatal("auto mode should drop messages without parts")
	}
}

func TestJoinBuilds(t *testing.T) {
	cases := []struct {
		build string
		check func(t *testing.T, got interface{})
	}{
		{"string", func(t *testing.T, got interface{}) {
			if got != "a-b" {
				t.Fatalf("string join wrong: %v", got)
			}
		}},
		{"object", func(t *testing.T, got interface{}) {
			m, ok := got.(map[string]interface{})
			if !ok || m["0"] != "a" || m["1"] != "b" {
				t.Fatalf("object join wrong: %v", got)
			}
		}},
		{"merged", func(t *testing.T, got interface{}) {
			m, ok := got.(map[string]interface{})
			if !ok || m["x"] != 1 || m["y"] != 2 {
				t.Fatalf("merged join wrong: %v", got)
			}
		}},
	}
	for _, c := range cases {
		host := newFakeHost()
		n := newNode(&JoinConfig{}, host) // auto mode: parts.type drives the build
		ctx := context.Background()
		var p1, p2 interface{} = "a", "b"
		if c.build == "merged" {
			// merged counts distinct keys — two one-key maps complete at 2
			p1 = map[string]interface{}{"x": 1}
			p2 = map[string]interface{}{"y": 2}
		}
		extra := map[string]interface{}{"type": c.build}
		if c.build == "string" {
			extra["ch"] = "-"
		}
		if c.build == "object" {
			extra["key"] = "0"
		}
		mk := func(i int, v interface{}, ex map[string]interface{}) nodestore.MessageInterface {
			m := partMsg("g", i, 2, v)
			for k, ev := range ex {
				m.GetMetadata()["parts"].(map[string]interface{})[k] = ev
			}
			return m
		}
		if _, err := n.Execute(ctx, mk(0, p1, extra)); err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		extra["key"] = "1"
		out, err := n.Execute(ctx, mk(1, p2, extra))
		if err != nil || out == nil {
			t.Fatalf("%s: expected joined message", c.build)
		}
		c.check(t, out.GetPayload())
	}
}

func TestJoinObjectUsesPartsKey(t *testing.T) {
	host := newFakeHost()
	n := newNode(&JoinConfig{Build: "object"}, host)
	mk := func(key string, v interface{}) nodestore.MessageInterface {
		m := nodestore.NewMessage().SetPayload(v)
		m.SetMetadata(map[string]interface{}{
			"parts": map[string]interface{}{"id": "g", "index": 0, "count": 2, "key": key, "type": "object"},
		})
		return m
	}
	n.Execute(context.Background(), mk("k1", "v1"))
	out, _ := n.Execute(context.Background(), mk("k2", "v2"))
	m := out.GetPayload().(map[string]interface{})
	if m["k1"] != "v1" || m["k2"] != "v2" {
		t.Fatalf("object join should use parts.key: %v", m)
	}
}

func TestJoinCustom(t *testing.T) {
	host := newFakeHost()
	n := newNode(&JoinConfig{Mode: "custom", Count: 2, Key: "topic"}, host)
	ctx := context.Background()
	mk := func(topic string, v interface{}) nodestore.MessageInterface {
		return nodestore.NewMessage().SetPayload(map[string]interface{}{"topic": topic, "v": v})
	}
	// Node-RED custom mode keeps a single group ("_"), not per-topic:
	// the first two arrivals complete regardless of their topics.
	out, _ := n.Execute(ctx, mk("b", "x"))
	if out != nil {
		t.Fatal("first message should buffer")
	}
	out, err := n.Execute(ctx, mk("a", 1))
	if err != nil || out == nil {
		t.Fatal("second message should complete the group")
	}
	// property default "payload": joined value is array of whole payloads
	arr := out.GetPayload().([]interface{})
	if len(arr) != 2 {
		t.Fatalf("custom join wrong: %v", arr)
	}
}

func TestJoinCustomNoCount(t *testing.T) {
	host := newFakeHost()
	n := newNode(&JoinConfig{Mode: "custom"}, host)
	// Node-RED buffers indefinitely with no count/timeout — no error
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{}))
	if err != nil || out != nil {
		t.Fatal("custom mode without count should buffer silently")
	}
}

func TestJoinTimeoutFlush(t *testing.T) {
	host := newFakeHost()
	n := newNode(&JoinConfig{Mode: "custom", Count: 5, Timeout: 0.02}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{"topic": "t", "v": 1})); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(host.emittedMsgs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	emitted := host.emittedMsgs()
	if len(emitted) != 1 {
		t.Fatal("timeout flush should emit the partial group")
	}
	if arr := emitted[0].GetPayload().([]interface{}); len(arr) != 1 {
		t.Fatalf("flushed group wrong: %v", arr)
	}
}

func TestJoinValidate(t *testing.T) {
	host := newFakeHost()
	if err := newNode(&JoinConfig{Mode: "auto", Build: "array"}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	// NOTE: mode()/build() normalize unknown values to defaults, so the
	// Validate switch can never actually fire — bogus values are accepted
	// and degrade to auto/array (documented; flagged in task evidence).
	if err := newNode(&JoinConfig{Mode: "bogus"}, host).Validate(); err != nil {
		t.Fatalf("bogus mode degrades to auto, should validate: %v", err)
	}
	if err := newNode(&JoinConfig{Build: "bogus"}, host).Validate(); err != nil {
		t.Fatalf("bogus build degrades to array, should validate: %v", err)
	}
}

func TestJoinPropertyPath(t *testing.T) {
	host := newFakeHost()
	n := newNode(&JoinConfig{}, host)
	ctx := context.Background()
	// auto mode reads/writes the path in parts.property (Node-RED)
	mk := func(i int, v interface{}) nodestore.MessageInterface {
		m := nodestore.NewMessage().SetPayload(map[string]interface{}{"items": v})
		m.SetMetadata(map[string]interface{}{
			"parts": map[string]interface{}{"id": "g", "index": i, "count": 2, "property": "payload.items"},
		})
		return m
	}
	n.Execute(ctx, mk(0, "a"))
	out, _ := n.Execute(ctx, mk(1, "b"))
	m := out.GetPayload().(map[string]interface{})
	arr := m["items"].([]interface{})
	if len(arr) != 2 || arr[0] != "a" {
		t.Fatalf("joined at wrong path: %v", m)
	}
}
