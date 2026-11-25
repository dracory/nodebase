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
type fakeHost struct {
	mu      sync.Mutex // guards states and emitted — timer goroutines emit
	states  map[string]interface{}
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
	n, _ := newNode(&DelayConfig{Duration: 1})
	in := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), in)
	if err != nil || out != in {
		t.Fatalf("delay should pass the input through: %v", err)
	}
}

func TestDelayTimeoutUnits(t *testing.T) {
	n, _ := newNode(&DelayConfig{PauseType: "delay", Timeout: "30", TimeoutUnits: "milliseconds"})
	start := time.Now()
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil || out == nil {
		t.Fatalf("delay failed: %v", err)
	}
	if d := time.Since(start); d < 25*time.Millisecond {
		t.Fatalf("returned too early: %v", d)
	}
}

func TestDelayCancelled(t *testing.T) {
	n, _ := newNode(&DelayConfig{PauseType: "delay", Timeout: 60, TimeoutUnits: "seconds"})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	if _, err := n.Execute(ctx, nodestore.NewMessage()); err == nil {
		t.Fatal("cancelled delay should error")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("cancelled delay should return promptly")
	}
}

func TestDelayVariable(t *testing.T) {
	n, _ := newNode(&DelayConfig{PauseType: "delayv", DelayPath: "delay"})
	in := nodestore.NewMessage().SetMetadata(map[string]interface{}{"delay": "30"})
	start := time.Now()
	out, err := n.Execute(context.Background(), in)
	if err != nil || out == nil {
		t.Fatalf("delayv failed: %v", err)
	}
	if d := time.Since(start); d < 25*time.Millisecond {
		t.Fatalf("delayv returned too early: %v", d)
	}
}

func TestDelayRandom(t *testing.T) {
	n, _ := newNode(&DelayConfig{
		PauseType: "random", RandomFirst: 10, RandomLast: 30, RandomUnits: "milliseconds",
	})
	for i := 0; i < 5; i++ {
		start := time.Now()
		if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d < 8*time.Millisecond || d > 500*time.Millisecond {
			t.Fatalf("random delay out of range: %v", d)
		}
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
