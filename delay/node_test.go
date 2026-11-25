package delay

// Pack-local unit tests for the delay node — fixed/variable/random
// delays, rate limiting (queue, drop, per-topic, msg.rate override,
// reset, flush, toFront) and context cancellation.

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
// emitted-message capture for the timed rate-limiter releases.
// emitRecord pairs an emitted message with the ports it was sent on
// (nil = all outgoing edges).
type emitRecord struct {
	msg   nodestore.MessageInterface
	ports []string
}

type fakeHost struct {
	mu      sync.Mutex // guards states and emitted — timer goroutines emit
	states  map[string]interface{}
	emitted []emitRecord
}

func newFakeHost() *fakeHost { return &fakeHost{states: map[string]interface{}{}} }

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, emitRecord{msg: p, ports: ports})
	return nil
}
func (h *fakeHost) emittedMsgs() []nodestore.MessageInterface {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]nodestore.MessageInterface, 0, len(h.emitted))
	for _, e := range h.emitted {
		out = append(out, e.msg)
	}
	return out
}
func (h *fakeHost) emittedRecs() []emitRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]emitRecord(nil), h.emitted...)
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

func newNode(cfg *DelayConfig) (*DelayNode, *fakeHost) {
	host := newFakeHost()
	return NewDelayNode(nodestore.NewNode("d1", "delay").SetWorkflowID("wf"), cfg, host), host
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestDelayPasses(t *testing.T) {
	n, host := newNode(&DelayConfig{Duration: 1})
	in := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), in)
	if err != nil || out != nil {
		t.Fatalf("delay holds the message asynchronously — out=%v err=%v", out, err)
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "held release")
	if got := host.emittedMsgs()[0]; got.GetPayload() != "x" {
		t.Fatalf("released message lost its payload: %v", got.GetPayload())
	}
}

func TestDelayTimeoutUnits(t *testing.T) {
	n, host := newNode(&DelayConfig{PauseType: "delay", Timeout: "30", TimeoutUnits: "milliseconds"})
	start := time.Now()
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil || out != nil {
		t.Fatalf("delay should not block or error: out=%v err=%v", out, err)
	}
	if d := time.Since(start); d > 20*time.Millisecond {
		t.Fatalf("execute should return immediately, took %v", d)
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "held release")
}

func TestDelayReleasedOnTimer(t *testing.T) {
	// A held message is parked until its timer fires — sibling work is
	// not stalled while it waits (Node-RED semantics).
	n, host := newNode(&DelayConfig{PauseType: "delay", Timeout: 80, TimeoutUnits: "milliseconds"})
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil || out != nil {
		t.Fatalf("delay should return nil immediately: out=%v err=%v", out, err)
	}
	time.Sleep(20 * time.Millisecond)
	if len(host.emittedMsgs()) != 0 {
		t.Fatal("held message released before its timer")
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "held release")
}

func TestDelayVariable(t *testing.T) {
	n, host := newNode(&DelayConfig{PauseType: "delayv", DelayPath: "delay", Timeout: 60, TimeoutUnits: "seconds"})
	in := nodestore.NewMessage().SetMetadata(map[string]interface{}{"delay": "30"})
	out, err := n.Execute(context.Background(), in)
	if err != nil || out != nil {
		t.Fatalf("delayv should not block or error: out=%v err=%v", out, err)
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "msg.delay release")
}

func TestDelayRandom(t *testing.T) {
	n, host := newNode(&DelayConfig{
		PauseType: "random", RandomFirst: 10, RandomLast: 30, RandomUnits: "milliseconds",
	})
	for i := 0; i < 5; i++ {
		out, err := n.Execute(context.Background(), nodestore.NewMessage())
		if err != nil || out != nil {
			t.Fatalf("random delay should not block or error: out=%v err=%v", out, err)
		}
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 5 }, "random releases")
}

func TestDelayResetClearsHeld(t *testing.T) {
	// msg.reset drops every parked message — including the reset
	// message's own pending release — without sending anything.
	n, host := newNode(&DelayConfig{PauseType: "delay", Timeout: 60, TimeoutUnits: "seconds"})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("held"))
	out, _ := n.Execute(ctx, nodestore.NewMessage().SetMetadata(map[string]interface{}{"reset": true}))
	if out != nil {
		t.Fatal("reset message should be consumed")
	}
	time.Sleep(100 * time.Millisecond)
	if len(host.emittedMsgs()) != 0 {
		t.Fatal("reset should clear held messages without sending them")
	}
}

func TestDelayFlushReleasesHeld(t *testing.T) {
	// A payload-carrying flush message is parked first, then flushes
	// the whole held list — including itself (Node-RED semantics).
	n, host := newNode(&DelayConfig{PauseType: "delay", Timeout: 60, TimeoutUnits: "seconds"})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("held"))
	out, _ := n.Execute(ctx, nodestore.NewMessage().SetPayload("f").
		SetMetadata(map[string]interface{}{"flush": true}))
	if out != nil {
		t.Fatal("flush message should be consumed")
	}
	em := host.emittedMsgs()
	if len(em) != 2 || em[0].GetPayload() != "held" || em[1].GetPayload() != "f" {
		t.Fatalf("flush should release held messages FIFO, got %v", em)
	}
}

func TestDelayPureFlushNotHeld(t *testing.T) {
	// A bare flush control message (no payload, no other properties)
	// releases the held list without entering it.
	n, host := newNode(&DelayConfig{PauseType: "delay", Timeout: 60, TimeoutUnits: "seconds"})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("held"))
	out, _ := n.Execute(ctx, nodestore.NewMessage().SetMetadata(map[string]interface{}{"flush": true}))
	if out != nil {
		t.Fatal("flush control should be consumed")
	}
	em := host.emittedMsgs()
	if len(em) != 1 || em[0].GetPayload() != "held" {
		t.Fatalf("pure flush should release only the held message, got %v", em)
	}
}

func TestRateLimitSpreads(t *testing.T) {
	// 1 msg per 50ms window — first passes, second is held and emitted.
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 50, RateUnits: "milliseconds",
	})
	ctx := context.Background()
	if out, _ := n.Execute(ctx, nodestore.NewMessage().SetPayload("a")); out == nil {
		t.Fatal("first message should pass immediately")
	}
	if out, _ := n.Execute(ctx, nodestore.NewMessage().SetPayload("b")); out != nil {
		t.Fatal("second message inside the window should be queued")
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "queued release")
	if got := host.emittedMsgs()[0].GetPayload(); got != "b" {
		t.Fatalf("released payload = %v", got)
	}
}

func TestRateLimitQueueFIFO(t *testing.T) {
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 30, RateUnits: "milliseconds",
	})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("1"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("2"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("3"))
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 2 }, "two queued releases")
	em := host.emittedMsgs()
	if em[0].GetPayload() != "2" || em[1].GetPayload() != "3" {
		t.Fatalf("FIFO order broken: %v %v", em[0].GetPayload(), em[1].GetPayload())
	}
}

func TestRateLimitDropKeepsLatest(t *testing.T) {
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 40, RateUnits: "milliseconds", Drop: true,
	})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("1"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("2")) // dropped — replaced
	n.Execute(ctx, nodestore.NewMessage().SetPayload("3")) // latest kept
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "latest release")
	if got := host.emittedMsgs()[0].GetPayload(); got != "3" {
		t.Fatalf("drop mode should release the latest, got %v", got)
	}
}

func TestRateLimitTopicMode(t *testing.T) {
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 40, RateUnits: "milliseconds", RateType: "topic",
	})
	ctx := context.Background()
	meta := func(topic string) map[string]interface{} { return map[string]interface{}{"topic": topic} }
	// Different topics have independent windows.
	if out, _ := n.Execute(ctx, nodestore.NewMessage().SetPayload("a1").SetMetadata(meta("a"))); out == nil {
		t.Fatal("first msg of topic a should pass")
	}
	if out, _ := n.Execute(ctx, nodestore.NewMessage().SetPayload("b1").SetMetadata(meta("b"))); out == nil {
		t.Fatal("first msg of topic b should pass — independent bucket")
	}
	// Intermediates per topic auto-drop; latest per topic is released.
	n.Execute(ctx, nodestore.NewMessage().SetPayload("a2").SetMetadata(meta("a")))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("a3").SetMetadata(meta("a")))
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "topic release")
	if got := host.emittedMsgs()[0].GetPayload(); got != "a3" {
		t.Fatalf("topic mode should keep the latest per topic, got %v", got)
	}
}

func TestRateLimitReset(t *testing.T) {
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 60, RateUnits: "milliseconds",
	})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("1"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("held"))
	out, _ := n.Execute(ctx, nodestore.NewMessage().SetMetadata(map[string]interface{}{"reset": true}))
	if out != nil {
		t.Fatal("reset message should be consumed")
	}
	time.Sleep(150 * time.Millisecond)
	if len(host.emittedMsgs()) != 0 {
		t.Fatal("reset should clear held messages without sending them")
	}
}

func TestRateLimitFlush(t *testing.T) {
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 500, RateUnits: "milliseconds",
	})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("1"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("2"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("3"))
	n.Execute(ctx, nodestore.NewMessage().SetMetadata(map[string]interface{}{"flush": 1}))
	if em := host.emittedMsgs(); len(em) != 1 || em[0].GetPayload() != "2" {
		t.Fatalf("flush=1 should release one held message, got %v", em)
	}
	n.Execute(ctx, nodestore.NewMessage().SetMetadata(map[string]interface{}{"flush": true}))
	if em := host.emittedMsgs(); len(em) != 2 || em[1].GetPayload() != "3" {
		t.Fatalf("flush=true should release all held, got %v", em)
	}
}

func TestRateLimitToFront(t *testing.T) {
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 50, RateUnits: "milliseconds",
	})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("1"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("2"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("vip").
		SetMetadata(map[string]interface{}{"toFront": true}))
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "toFront release")
	if got := host.emittedMsgs()[0].GetPayload(); got != "vip" {
		t.Fatalf("toFront should release first, got %v", got)
	}
}

func TestDelayUnitDays(t *testing.T) {
	// Node-RED accepts "days" for delay units and "day" for rate
	// units — both spellings resolve to 24 hours.
	for _, u := range []string{"day", "days", "DAYS"} {
		if d := delayUnit(u); d != 24*time.Hour {
			t.Fatalf("delayUnit(%q) = %v, want 24h", u, d)
		}
	}
}

func TestRateLimitDroppedToOutput2(t *testing.T) {
	// outputs:2 turns on Node-RED's burst behaviour — a message
	// displaced by a newer arrival in drop mode is emitted on
	// output_2 instead of being discarded, while paced releases
	// stay on output_1.
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 60, RateUnits: "milliseconds",
		Drop: true, Outputs: 2,
	})
	ctx := context.Background()
	if out, _ := n.Execute(ctx, nodestore.NewMessage().SetPayload("1")); out == nil {
		t.Fatal("first message should pass immediately")
	}
	n.Execute(ctx, nodestore.NewMessage().SetPayload("2")) // kept as latest
	n.Execute(ctx, nodestore.NewMessage().SetPayload("3")) // displaces "2"
	waitFor(t, func() bool { return len(host.emittedRecs()) == 1 }, "dropped emit")
	rec := host.emittedRecs()[0]
	if rec.msg.GetPayload() != "2" || len(rec.ports) != 1 || rec.ports[0] != "output_2" {
		t.Fatalf("displaced message should emit on output_2, got payload=%v ports=%v",
			rec.msg.GetPayload(), rec.ports)
	}
	waitFor(t, func() bool { return len(host.emittedRecs()) == 2 }, "paced release")
	rec = host.emittedRecs()[1]
	if rec.msg.GetPayload() != "3" || len(rec.ports) != 1 || rec.ports[0] != "output_1" {
		t.Fatalf("paced release should emit on output_1, got payload=%v ports=%v",
			rec.msg.GetPayload(), rec.ports)
	}
}

func TestRateLimitDroppedDiscardedWithoutOutput2(t *testing.T) {
	// Without the second output a displaced message is discarded —
	// nothing is emitted for it.
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 40, RateUnits: "milliseconds", Drop: true,
	})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("1"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("2"))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("3"))
	waitFor(t, func() bool { return len(host.emittedRecs()) == 1 }, "latest release")
	if rec := host.emittedRecs()[0]; rec.msg.GetPayload() != "3" {
		t.Fatalf("only the latest should release, got %v", rec.msg.GetPayload())
	}
}

func TestRateLimitDroppedToOutput2TopicMode(t *testing.T) {
	// Per-topic mode: a queued message replaced by a newer arrival on
	// the same topic emits on output_2 (Node-RED parity).
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 60, RateUnits: "milliseconds",
		RateType: "topic", Outputs: 2,
	})
	ctx := context.Background()
	meta := map[string]interface{}{"topic": "a"}
	n.Execute(ctx, nodestore.NewMessage().SetPayload("a1").SetMetadata(meta))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("a2").SetMetadata(meta))
	n.Execute(ctx, nodestore.NewMessage().SetPayload("a3").SetMetadata(meta))
	waitFor(t, func() bool { return len(host.emittedRecs()) == 1 }, "dropped emit")
	rec := host.emittedRecs()[0]
	if rec.msg.GetPayload() != "a2" || len(rec.ports) != 1 || rec.ports[0] != "output_2" {
		t.Fatalf("replaced topic message should emit on output_2, got payload=%v ports=%v",
			rec.msg.GetPayload(), rec.ports)
	}
}

func TestRateLimitMsgRateOverride(t *testing.T) {
	n, host := newNode(&DelayConfig{
		PauseType: "rate", Rate: 1, NbRateUnits: 60, RateUnits: "seconds", AllowRate: true,
	})
	ctx := context.Background()
	n.Execute(ctx, nodestore.NewMessage().SetPayload("1"))
	// msg.rate=30 (ms) shortens the interval so the queued msg releases soon.
	n.Execute(ctx, nodestore.NewMessage().SetPayload("2").
		SetMetadata(map[string]interface{}{"rate": 30}))
	waitFor(t, func() bool { return len(host.emittedMsgs()) == 1 }, "override-timed release")
}
