package inject

// Internal-method coverage ported from flowrunner's node_inject_test.go
// (buildPayload / intervalUnit are unexported, so these live inside the
// node package rather than in noderunner_test).

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
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
	emitted []nodestore.MessageInterface
}

func newFakeHost() *fakeHost { return &fakeHost{states: map[string]interface{}{}} }

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
	return nil
}
func (h *fakeHost) emittedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.emitted)
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

func newHostedNode(cfg *InjectConfig) (*InjectNode, *fakeHost) {
	host := newFakeHost()
	return NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), cfg, host), host
}

// waitForEmit polls until the host has captured n messages or fails.
func waitForEmit(t *testing.T, h *fakeHost, n int, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.emittedCount() >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// hhmmss formats a wall-clock time as the "HH:MM:SS" schedule spec.
func hhmmss(t time.Time) string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}

// TestInjectPayloadTypes — NR-21: num/bool/env payload types.
func TestInjectPayloadTypes(t *testing.T) {

	build := func(cfg *InjectConfig) nodestore.MessageInterface {
		n := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), cfg, nil)
		// Execute materializes payloadType from the config
		if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		return n.buildPayload()
	}

	if d := build(&InjectConfig{Payload: "3.5", PayloadType: "num"}).GetPayload(); d != float64(3.5) {
		t.Fatalf("num payload: got %v", d)
	}
	if d := build(&InjectConfig{Payload: "true", PayloadType: "bool"}).GetPayload(); d != true {
		t.Fatalf("bool payload: got %v", d)
	}
	t.Setenv("NODEBASE_TEST_ENV", "seekrit")
	if d := build(&InjectConfig{Payload: "NODEBASE_TEST_ENV", PayloadType: "env"}).GetPayload(); d != "seekrit" {
		t.Fatalf("env payload: got %v", d)
	}
}

// TestInjectTopicAndUnits — NR-22: topic lands in Metadata, units scale.
func TestInjectTopicAndUnits(t *testing.T) {
	n := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), &InjectConfig{Topic: "sensors", PayloadType: "string", Payload: "x"}, nil)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := n.buildPayload().GetMetadata()["topic"]; got != "sensors" {
		t.Fatalf("expected topic sensors, got %v", got)
	}
	n2 := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), &InjectConfig{IntervalUnits: "hr"}, nil)
	if n2.intervalUnit() != time.Hour {
		t.Fatalf("expected hour unit, got %v", n2.intervalUnit())
	}
	n3 := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), &InjectConfig{}, nil)
	if n3.intervalUnit() != time.Second {
		t.Fatalf("expected second default, got %v", n3.intervalUnit())
	}
}

// TestInjectOnceDelay — the initial fire is delayed by onceDelay
// seconds (nothing before the delay, one message after).
func TestInjectOnceDelay(t *testing.T) {
	n, host := newHostedNode(&InjectConfig{Once: true, OnceDelay: 0.15, PayloadType: "string", Payload: "x"})
	defer n.Stop()
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if c := host.emittedCount(); c != 0 {
		t.Fatalf("fired before onceDelay elapsed (%d emits)", c)
	}
	waitForEmit(t, host, 1, "delayed once fire")
}

// TestInjectOnceImmediate — no onceDelay fires right away.
func TestInjectOnceImmediate(t *testing.T) {
	n, host := newHostedNode(&InjectConfig{Once: true, PayloadType: "string", Payload: "x"})
	defer n.Stop()
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	waitForEmit(t, host, 1, "immediate once fire")
}

// TestInjectRepeatField — Node-RED's repeat field (seconds, often a
// string) drives the interval when Interval is unset.
func TestInjectRepeatField(t *testing.T) {
	n, host := newHostedNode(&InjectConfig{Repeat: "0.05", PayloadType: "string", Payload: "x"})
	defer n.Stop()
	if got := n.repeatInterval(); got != 50*time.Millisecond {
		t.Fatalf("expected 50ms repeat interval, got %v", got)
	}
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	waitForEmit(t, host, 2, "repeat-field fires")
}

// TestInjectTimeOfDayFires — a time-of-day entry a couple of seconds
// ahead fires on schedule.
func TestInjectTimeOfDayFires(t *testing.T) {
	at := time.Now().Add(2 * time.Second)
	n, host := newHostedNode(&InjectConfig{
		Times:       []InjectTime{{Time: hhmmss(at)}},
		PayloadType: "string",
		Payload:     "x",
	})
	defer n.Stop()
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if c := host.emittedCount(); c != 0 {
		t.Fatalf("fired too early (%d emits)", c)
	}
	waitForEmit(t, host, 1, "time-of-day fire")
}

// TestInjectTimeOfDayWeekdaySkip — an entry whose weekday filter
// excludes today does not fire promptly.
func TestInjectTimeOfDayWeekdaySkip(t *testing.T) {
	at := time.Now().Add(2 * time.Second)
	otherDay := (int(at.Weekday()) + 1) % 7
	n, host := newHostedNode(&InjectConfig{
		Times:       []InjectTime{{Time: hhmmss(at), Days: []int{otherDay}}},
		PayloadType: "string",
		Payload:     "x",
	})
	defer n.Stop()
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	// Edge case: the target time may cross midnight, making tomorrow
	// an allowed day — only assert within the pre-fire window.
	time.Sleep(300 * time.Millisecond)
	if at.Day() == time.Now().Day() {
		if c := host.emittedCount(); c != 0 {
			t.Fatalf("fired on a disallowed weekday (%d emits)", c)
		}
	}
}

// TestNextTimeOfDay — weekday filtering math of the scheduler.
func TestNextTimeOfDay(t *testing.T) {
	// 2026-11-23 is a Monday.
	monday := time.Date(2026, 11, 23, 10, 0, 0, 0, time.Local)

	// Later today on an allowed day.
	d := nextTimeOfDay(monday, timeEntry{hour: 11, min: 30, days: map[int]bool{1: true}})
	if d != 90*time.Minute {
		t.Fatalf("expected 90m wait, got %v", d)
	}

	// Wednesday-only entry from Monday → ~2 days.
	d = nextTimeOfDay(monday, timeEntry{hour: 10, min: 0, sec: 1, days: map[int]bool{3: true}})
	want := (48*time.Hour + time.Second)
	if d != want {
		t.Fatalf("expected %v wait, got %v", want, d)
	}

	// Time already passed today (unfiltered) → tomorrow.
	d = nextTimeOfDay(monday, timeEntry{hour: 9, min: 0})
	if d != 23*time.Hour {
		t.Fatalf("expected 23h wait, got %v", d)
	}

	// Day-of-month boundary: weekday "0" = Sunday is skipped on Monday.
	d = nextTimeOfDay(monday, timeEntry{hour: 10, min: 0, sec: 1, days: map[int]bool{0: true}})
	if d != 6*24*time.Hour+time.Second {
		t.Fatalf("expected ~6d wait, got %v", d)
	}
}

// TestParseCrontab — Node-RED crontab strings map onto time entries.
func TestParseCrontab(t *testing.T) {
	entries := parseCrontab("00 16 * * 1,2,3,4,5")
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.hour != 16 || e.min != 0 {
		t.Fatalf("expected 16:00, got %02d:%02d", e.hour, e.min)
	}
	for _, wd := range []int{1, 2, 3, 4, 5} {
		if !e.days[wd] {
			t.Fatalf("weekday %d missing from %v", wd, e.days)
		}
	}
	if e.days[0] || e.days[6] {
		t.Fatalf("weekend should be excluded: %v", e.days)
	}

	// Hour lists expand to one entry each.
	if got := len(parseCrontab("30 9,17 * * *")); got != 2 {
		t.Fatalf("expected 2 entries for hour list, got %d", got)
	}

	// Day ranges work too.
	entries = parseCrontab("0 9 * * 1-5")
	if len(entries) != 1 || len(entries[0].days) != 5 {
		t.Fatalf("expected weekday-range entry, got %v", entries)
	}

	// Six fields: leading seconds are honoured.
	entries = parseCrontab("15 30 8 * * *")
	if len(entries) != 1 || entries[0].sec != 15 || entries[0].hour != 8 || entries[0].min != 30 {
		t.Fatalf("expected 08:30:15, got %v", entries)
	}

	// Unparseable / non-time schedules are skipped.
	for _, bad := range []string{"", "nonsense", "*/10 9-17 * * 1-5", "0 9 1 * *"} {
		if got := parseCrontab(bad); len(got) != 0 {
			t.Fatalf("expected no entries for %q, got %v", bad, got)
		}
	}
}

// TestInjectCrontabConfig — a Node-RED imported config (crontab only)
// resolves to a times schedule via the inference path.
func TestInjectCrontabConfig(t *testing.T) {
	n, _ := newHostedNode(&InjectConfig{Crontab: "00 16 * * 1,2,3,4,5"})
	if got := n.repeatMode(); got != "times" {
		t.Fatalf("expected times mode, got %q", got)
	}
	entries := n.timeEntries()
	if len(entries) != 1 || entries[0].hour != 16 {
		t.Fatalf("expected 16:00 entry, got %v", entries)
	}
}

// TestInjectOnceDelayString — onceDelay accepts quoted values like
// other numeric fields (Node-RED serializes loosely).
func TestInjectOnceDelayString(t *testing.T) {
	n, _ := newHostedNode(&InjectConfig{OnceDelay: "2"})
	if got := n.onceDelay(); got != 2*time.Second {
		t.Fatalf("expected 2s onceDelay, got %v", got)
	}
}

// TestPropsLegacyFallback ? Node-RED migrated flows carry props entries
// without a v key ({"p":"payload"} / {"p":"topic","vt":"str"}); the runtime
// then falls back to the legacy top-level payload/payloadType/topic fields.
// An explicit empty v still wins over the legacy field.
func TestPropsLegacyFallback(t *testing.T) {
	raw := `{"topic":"Fruits","payload":"[1]","payloadType":"json","props":[{"p":"payload"},{"p":"topic","vt":"str"}]}`
	cfg := new(InjectConfig)
	if err := json.Unmarshal([]byte(raw), cfg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	n := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), cfg, nil)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	msg := n.buildPayload()
	if got := msg.GetMetadata()["topic"]; got != "Fruits" {
		t.Fatalf("topic should fall back to the legacy field, got %v", got)
	}
	payload, _ := msg.GetPayload().([]interface{})
	if len(payload) != 1 {
		t.Fatalf("payload should fall back to the legacy json field, got %v", msg.GetPayload())
	}
}

// TestPropsExplicitEmptyWins ? a props entry with an explicit (empty) v
// overrides the legacy top-level field rather than falling back to it.
func TestPropsExplicitEmptyWins(t *testing.T) {
	raw := `{"topic":"Fruits","payload":"x","payloadType":"str","props":[{"p":"topic","v":"","vt":"str"}]}`
	cfg := new(InjectConfig)
	if err := json.Unmarshal([]byte(raw), cfg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	n := NewInjectNode(nodestore.NewNode("n", "inject").SetWorkflowID("wf"), cfg, nil)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := n.buildPayload().GetMetadata()["topic"]; got != nil {
		t.Fatalf("explicit empty topic should win over legacy, got %v", got)
	}
}
