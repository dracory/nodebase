package trigger

// Internal coverage ported from flowrunner's node_trigger_test.go
// (config field and triggerValue are unexported).

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

// fakeHost implements nodebase.NodeHost with a state slot and an
// emitted-message capture for the scheduled fires.
type fakeHost struct {
	mu      sync.Mutex // guards states and emitted — timer goroutines emit
	states  map[string]interface{}
	emitted []emittedMsg
}

type emittedMsg struct {
	msg   nodestore.MessageInterface
	ports []string
}

func newFakeHost() *fakeHost { return &fakeHost{states: map[string]interface{}{}} }

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, emittedMsg{msg: p, ports: ports})
	return nil
}
func (h *fakeHost) emittedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.emitted)
}
func (h *fakeHost) emittedMessages() []emittedMsg {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]emittedMsg, len(h.emitted))
	copy(out, h.emitted)
	return out
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
	h.mu.Lock()
	defer h.mu.Unlock()
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

// newHostedNode builds a trigger node against the fake host so timer
// emissions can be captured.
func newHostedNode(t *testing.T, data map[string]interface{}) (*TriggerNode, *fakeHost) {
	t.Helper()
	reg, ok := nodebase.RegisteredNode("trigger")
	if !ok {
		t.Fatal("trigger type not registered")
	}
	host := newFakeHost()
	executor, err := reg.Factory(nodestore.NewNode("trg", "trigger").SetWorkflowID("wf").SetData(data), host)
	if err != nil {
		t.Fatalf("factory failed: %v", err)
	}
	trg, ok := executor.(*TriggerNode)
	if !ok {
		t.Fatalf("expected *TriggerNode, got %T", executor)
	}
	return trg, host
}

// waitForEmit polls until the host has captured n messages or fails.
func waitForEmit(t *testing.T, h *fakeHost, n int, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.emittedCount() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestTriggerNodeConfigDecode verifies the executor is built from node
// Data with the documented field names.
func TestTriggerNodeConfigDecode(t *testing.T) {
	reg, ok := nodebase.RegisteredNode("trigger")
	if !ok {
		t.Fatal("trigger type not registered")
	}
	executor, err := reg.Factory(nodestore.NewNode("trg", "trigger").SetData(map[string]interface{}{
		"op1":           "1",
		"op1type":       "num",
		"op2":           "0",
		"op2type":       "num",
		"duration":      2.0,
		"units":         "s",
		"extend":        true,
		"overrideDelay": true,
		"outputs":       1,
		"bytopic":       true,
	}), nil)
	if err != nil {
		t.Fatalf("executorForNode failed: %v", err)
	}
	trg, ok2 := executor.(*TriggerNode)
	if !ok2 {
		t.Fatalf("expected *TriggerNode, got %T", executor)
	}
	if trg.config.Op1 != "1" || trg.config.Op1Type != "num" || trg.config.Op2 != "0" ||
		trg.config.Duration != 2.0 || trg.config.Units != "s" || !trg.config.Extend ||
		!trg.config.OverrideDelay || trg.config.Outputs != 1 || trg.config.ByTopic != TopicScope(true) {
		t.Fatalf("config decoded incorrectly: %+v", trg.config)
	}
}

// TestTriggerValue covers the op value type parsing.
func TestTriggerValue(t *testing.T) {
	input := nodestore.NewMessage().SetPayload(map[string]interface{}{"x": 1})
	cases := []struct {
		kind, raw string
		want      interface{}
	}{
		{"str", "abc", "abc"},
		{"", "abc", "abc"},
		{"num", "3.5", 3.5},
		{"num", "notanum", float64(0)},
		{"bool", "true", true},
		{"bool", "false", false},
		{"json", `{"a":1}`, map[string]interface{}{"a": float64(1)}},
	}
	for _, c := range cases {
		got := triggerValue(c.kind, c.raw, input)
		switch want := c.want.(type) {
		case map[string]interface{}:
			m, ok := got.(map[string]interface{})
			if !ok || m["a"] != want["a"] {
				t.Fatalf("triggerValue(%q,%q)=%v want %v", c.kind, c.raw, got, c.want)
			}
		default:
			if got != c.want {
				t.Fatalf("triggerValue(%q,%q)=%v want %v", c.kind, c.raw, got, c.want)
			}
		}
	}
	if got := triggerValue("payload", "", input); got == nil {
		t.Fatal("payload type should pass input.GetPayload() through")
	}
	if got := triggerValue("payload", "", nil); got != nil {
		t.Fatal("payload type with no input should yield nil")
	}
}

// TestThenModeBlock covers "wait to be reset": the first message sends
// op1 and blocks; further messages are dropped until a reset clears
// the topic. Node-RED serializes this mode as duration=0.
func TestThenModeBlock(t *testing.T) {
	for _, data := range []map[string]interface{}{
		{"op1": "go", "duration": 0.0},                  // Node-RED serialization
		{"op1": "go", "then": "block", "duration": 0.0}, // explicit then
	} {
		trg, _ := newHostedNode(t, data)
		msg := func() nodestore.MessageInterface { return nodestore.NewMessage().SetPayload("in") }

		out, err := trg.Execute(context.Background(), msg())
		if err != nil || out == nil || out.GetPayload() != "go" {
			t.Fatalf("first message should emit op1, got %v err %v (data %v)", out, err, data)
		}
		out, err = trg.Execute(context.Background(), msg())
		if err != nil || out != nil {
			t.Fatalf("blocked message should be dropped, got %v err %v (data %v)", out, err, data)
		}

		// Reset releases the block; the next message sends op1 again.
		reset := nodestore.NewMessage().SetMetadata(map[string]interface{}{"reset": true})
		out, err = trg.Execute(context.Background(), reset)
		if err != nil || out != nil {
			t.Fatalf("reset should forward nothing, got %v err %v", out, err)
		}
		out, err = trg.Execute(context.Background(), msg())
		if err != nil || out == nil || out.GetPayload() != "go" {
			t.Fatalf("post-reset message should emit op1, got %v err %v", out, err)
		}
	}
}

// TestThenModeBlockTimeout covers "wait to be reset" with a timeout:
// when a positive delay is configured the held block releases on its
// own and the second output fires.
func TestThenModeBlockTimeout(t *testing.T) {
	trg, host := newHostedNode(t, map[string]interface{}{
		"op1": "go", "then": "block", "op2": "done", "duration": 60.0, "units": "ms",
	})
	out, err := trg.Execute(context.Background(), nodestore.NewMessage().SetPayload("in"))
	if err != nil || out == nil || out.GetPayload() != "go" {
		t.Fatalf("first message should emit op1, got %v err %v", out, err)
	}
	// While blocked a second message is dropped.
	out, err = trg.Execute(context.Background(), nodestore.NewMessage().SetPayload("in2"))
	if err != nil || out != nil {
		t.Fatalf("blocked message should be dropped, got %v err %v", out, err)
	}
	waitForEmit(t, host, 1, "block timeout release")
	if got := host.emittedMessages()[0].msg.GetPayload(); got != "done" {
		t.Fatalf("timeout should emit op2, got %v", got)
	}
}

// TestThenModeLoop covers "resend it every X": op1 goes out
// immediately and again on every interval until a reset stops it.
// Node-RED serializes this mode as a negative duration.
func TestThenModeLoop(t *testing.T) {
	for _, data := range []map[string]interface{}{
		{"op1": "tick", "duration": -60.0, "units": "ms"},                // Node-RED serialization
		{"op1": "tick", "then": "loop", "duration": 60.0, "units": "ms"}, // explicit then
	} {
		trg, host := newHostedNode(t, data)
		out, err := trg.Execute(context.Background(), nodestore.NewMessage().SetPayload("in"))
		if err != nil || out == nil || out.GetPayload() != "tick" {
			t.Fatalf("loop should emit op1 immediately, got %v err %v (data %v)", out, err, data)
		}
		waitForEmit(t, host, 2, "two resends")
		for _, e := range host.emittedMessages() {
			if e.msg.GetPayload() != "tick" {
				t.Fatalf("resend should carry op1, got %v", e.msg.GetPayload())
			}
			if len(e.ports) != 1 || e.ports[0] != "output_1" {
				t.Fatalf("resends must leave on output_1, got %v", e.ports)
			}
		}
	}
}

// TestThenModeLoopReset stops a running resend loop via msg.reset.
func TestThenModeLoopReset(t *testing.T) {
	trg, host := newHostedNode(t, map[string]interface{}{
		"op1": "tick", "duration": -40.0, "units": "ms",
	})
	if _, err := trg.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatal(err)
	}
	waitForEmit(t, host, 1, "first resend")
	if _, err := trg.Execute(context.Background(), nodestore.NewMessage().SetMetadata(map[string]interface{}{"reset": true})); err != nil {
		t.Fatal(err)
	}
	got := host.emittedCount()
	time.Sleep(120 * time.Millisecond)
	if host.emittedCount() != got {
		t.Fatalf("loop kept resending after reset: %d -> %d", got, host.emittedCount())
	}
}

// TestThenModeLoopRestart verifies a new message restarts the resend
// interval and re-emits op1 (Node-RED semantics).
func TestThenModeLoopRestart(t *testing.T) {
	trg, host := newHostedNode(t, map[string]interface{}{
		"op1": "tick", "duration": -60.0, "units": "ms",
	})
	out, err := trg.Execute(context.Background(), nodestore.NewMessage().SetPayload("a"))
	if err != nil || out == nil {
		t.Fatal(err)
	}
	out, err = trg.Execute(context.Background(), nodestore.NewMessage().SetPayload("b"))
	if err != nil || out == nil || out.GetPayload() != "tick" {
		t.Fatalf("new message should re-emit op1, got %v err %v", out, err)
	}
	waitForEmit(t, host, 1, "restarted interval")
	if _, err := trg.Execute(context.Background(), nodestore.NewMessage().SetMetadata(map[string]interface{}{"reset": true})); err != nil {
		t.Fatal(err)
	}
}

// TestResetByPayload covers the reset/resetValue config: a message
// whose payload stringifies to the configured value resets pending
// timers and forwards nothing.
func TestResetByPayload(t *testing.T) {
	for _, key := range []string{"reset", "resetValue"} {
		trg, host := newHostedNode(t, map[string]interface{}{
			"op1": "go", "op2": "done", "duration": 50.0, "units": "ms", key: "STOP",
		})
		if _, err := trg.Execute(context.Background(), nodestore.NewMessage().SetPayload("in")); err != nil {
			t.Fatal(err)
		}
		// A normal payload does not reset.
		out, err := trg.Execute(context.Background(), nodestore.NewMessage().SetPayload("other"))
		if err != nil || out == nil {
			t.Fatalf("non-matching payload must not reset (key %s)", key)
		}
		// Matching payload resets and forwards nothing.
		out, err = trg.Execute(context.Background(), nodestore.NewMessage().SetPayload("STOP"))
		if err != nil || out != nil {
			t.Fatalf("matching payload should reset and forward nothing, got %v (key %s)", out, key)
		}
		time.Sleep(100 * time.Millisecond)
		if host.emittedCount() != 0 {
			t.Fatalf("pending op2 should have been cleared by payload reset (key %s)", key)
		}
	}
}

// TestOpValueTypes covers the date/bin/env value types on op1/op2.
func TestOpValueTypes(t *testing.T) {
	t.Setenv("TRIGGER_TEST_ENV", "envvalue")

	trg, _ := newHostedNode(t, map[string]interface{}{"op1type": "date"})
	out, err := trg.Execute(context.Background(), nodestore.NewMessage())
	if err != nil || out == nil {
		t.Fatal(err)
	}
	ms, ok := out.GetPayload().(float64)
	if !ok || ms <= 0 {
		t.Fatalf("date type should emit ms timestamp, got %v", out.GetPayload())
	}
	if delta := float64(time.Now().UnixMilli()) - ms; delta < 0 || delta > 5000 {
		t.Fatalf("date timestamp not current: delta %v", delta)
	}

	// bin: Node-RED serializes bytes as a JSON array of codes; a
	// base64 string is accepted too.
	for raw, want := range map[string]string{
		"[104,105]": "hi",
		"aGk=":      "hi",
	} {
		got := triggerValue("bin", raw, nil)
		b, ok := got.([]byte)
		if !ok || string(b) != want {
			t.Fatalf("bin %q: got %v want %q", raw, got, want)
		}
	}

	trg, _ = newHostedNode(t, map[string]interface{}{"op1type": "env", "op1": "TRIGGER_TEST_ENV"})
	out, err = trg.Execute(context.Background(), nodestore.NewMessage())
	if err != nil || out == nil {
		t.Fatal(err)
	}
	if out.GetPayload() != "envvalue" {
		t.Fatalf("env type should read the variable, got %v", out.GetPayload())
	}

	// op2 types validate too: an unknown type must still fail loud.
	for _, tc := range []struct{ field, val string }{
		{"op1type", "bogus"}, {"op2type", "bogus"},
	} {
		trg, _ := newHostedNode(t, map[string]interface{}{tc.field: tc.val})
		if err := trg.Validate(); err == nil {
			t.Fatalf("%s=%q should fail validation", tc.field, tc.val)
		}
	}
	for _, kind := range []string{"str", "num", "bool", "json", "payload", "date", "bin", "env"} {
		trg, _ := newHostedNode(t, map[string]interface{}{"op1type": kind, "op2type": kind})
		if err := trg.Validate(); err != nil {
			t.Fatalf("type %q should validate: %v", kind, err)
		}
	}
}

// TestThenFieldDecode verifies the new config fields decode from node
// Data (both the Node-RED "reset" name and the "resetValue" alias).
func TestThenFieldDecode(t *testing.T) {
	trg, _ := newHostedNode(t, map[string]interface{}{
		"then": "loop", "reset": "R1", "resetValue": "R2",
	})
	if trg.config.Then != "loop" || trg.config.Reset != "R1" || trg.config.ResetValue != "R2" {
		t.Fatalf("then/reset fields decoded incorrectly: %+v", trg.config)
	}
}
