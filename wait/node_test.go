package wait

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

type memoryCoordinationStore struct {
	mu      sync.Mutex
	signals map[string]nodebase.CoordinationSignal
	waiters map[string]nodebase.CoordinationWaiter
}

func newMemoryCoordinationStore() *memoryCoordinationStore {
	return &memoryCoordinationStore{
		signals: map[string]nodebase.CoordinationSignal{},
		waiters: map[string]nodebase.CoordinationWaiter{},
	}
}

func (s *memoryCoordinationStore) IncrementSignal(ctx context.Context, key string, metadata map[string]interface{}, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	signal := s.signals[key]
	signal.Key = key
	signal.Count++
	if signal.Metadata == nil {
		signal.Metadata = map[string]interface{}{}
	}
	for k, v := range metadata {
		signal.Metadata[k] = v
	}
	s.signals[key] = signal
	return nil
}

func (s *memoryCoordinationStore) ParkWaiter(ctx context.Context, waiter nodebase.CoordinationWaiter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiters[waiter.ID] = waiter
	return nil
}

func (s *memoryCoordinationStore) PollWaiters(ctx context.Context, workflowID, nodeID string) ([]nodebase.CoordinationWaiter, []nodebase.CoordinationWaiter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var released, expired []nodebase.CoordinationWaiter
	for id, waiter := range s.waiters {
		if waiter.WorkflowID != workflowID || waiter.NodeID != nodeID {
			continue
		}
		if !waiter.ExpiresAt.IsZero() && !waiter.ExpiresAt.After(time.Now().UTC()) {
			expired = append(expired, waiter)
			delete(s.waiters, id)
			continue
		}
		signal := s.signals[waiter.SignalKey]
		if signal.Count < waiter.TargetCount {
			continue
		}
		signal.Count -= waiter.TargetCount
		waiter.CachedMetadata = copyMetadata(signal.Metadata)
		if signal.Count == 0 {
			delete(s.signals, waiter.SignalKey)
		} else {
			s.signals[waiter.SignalKey] = signal
		}
		released = append(released, waiter)
		delete(s.waiters, id)
	}
	return released, expired, nil
}

func (s *memoryCoordinationStore) Signal(ctx context.Context, key string) (nodebase.CoordinationSignal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	signal, ok := s.signals[key]
	return signal, ok, nil
}

func (s *memoryCoordinationStore) Waiters(ctx context.Context, workflowID, nodeID string) ([]nodebase.CoordinationWaiter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []nodebase.CoordinationWaiter
	for _, waiter := range s.waiters {
		if waiter.WorkflowID == workflowID && waiter.NodeID == nodeID {
			out = append(out, waiter)
		}
	}
	return out, nil
}

func copyMetadata(in map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

type emitRecord struct {
	msg   nodestore.MessageInterface
	ports []string
}

type fakeHost struct {
	nodebase.NodeHost
	store   nodebase.CoordinationStore
	mu      sync.Mutex
	states  map[string]interface{}
	emitted []emitRecord
	runCtx  context.Context
	cancel  context.CancelFunc
}

func newFakeHost(store nodebase.CoordinationStore) *fakeHost {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeHost{store: store, states: map[string]interface{}{}, runCtx: ctx, cancel: cancel}
}

func (h *fakeHost) ResolvedCoordinationStore() nodebase.CoordinationStore { return h.store }
func (h *fakeHost) RunContext(wf string) context.Context                  { return h.runCtx }
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string)     {}
func (h *fakeHost) Emit(wf, nodeID string, msg nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, emitRecord{msg: msg, ports: append([]string(nil), ports...)})
	return nil
}
func (h *fakeHost) GetOrCreateNodeState(wf, nodeID string, factory func() interface{}) interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := wf + "/" + nodeID
	if state, ok := h.states[key]; ok {
		return state
	}
	state := factory()
	h.states[key] = state
	return state
}
func (h *fakeHost) emittedRecords() []emitRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]emitRecord(nil), h.emitted...)
}

func waitTestNode(cfg *Config, host nodebase.NodeHost) *Node {
	return NewNode(nodestore.NewNode("w1", "wait").SetWorkflowID("wf"), cfg, host)
}

func waitConfig() *Config {
	return &Config{Signal: "job-{{payload.id}}", TargetCount: "{{payload.count}}", MergeMode: "wait-wins", PollIntervalMS: 20}
}

func TestWaitParksMessageWithoutPassingItThrough(t *testing.T) {
	store := newMemoryCoordinationStore()
	host := newFakeHost(store)
	defer host.cancel()
	node := waitTestNode(waitConfig(), host)
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"id": "42", "count": 2})
	out, err := node.Execute(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatalf("parked message must not pass through synchronously, got %#v", out)
	}
	waiters, err := store.Waiters(context.Background(), "wf", "w1")
	if err != nil || len(waiters) != 1 {
		t.Fatalf("expected one parked waiter: len=%d err=%v", len(waiters), err)
	}
	waiter := waiters[0]
	if waiter.SignalKey != "job-42" || waiter.TargetCount != 2 || waiter.Message != msg {
		t.Fatalf("parked waiter wrong: %+v", waiter)
	}
}

func TestWaitBlankSignalAndBadTargetFail(t *testing.T) {
	store := newMemoryCoordinationStore()
	host := newFakeHost(store)
	defer host.cancel()
	node := waitTestNode(&Config{Signal: "{{payload.missing}}", TargetCount: "1", MergeMode: "wait-wins"}, host)
	_, err := node.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{}))
	if err == nil || !strings.Contains(err.Error(), "blank") {
		t.Fatalf("blank signal should fail, got %v", err)
	}
	node = waitTestNode(&Config{Signal: "job", TargetCount: "{{payload.count}}", MergeMode: "wait-wins"}, host)
	_, err = node.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{"count": "zero"}))
	if err == nil || !strings.Contains(err.Error(), "positive integer") {
		t.Fatalf("bad target should fail, got %v", err)
	}
}

func TestWaitPollReleasesAndMergesCachedMetadata(t *testing.T) {
	store := newMemoryCoordinationStore()
	host := newFakeHost(store)
	defer host.cancel()
	cfg := waitConfig()
	node := waitTestNode(cfg, host)
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"id": "42", "count": 2}).SetMetadata(map[string]interface{}{
		"conflict": "waiting",
	})
	if _, err := node.Execute(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	cached := map[string]interface{}{"conflict": "notify", "worker": "alpha"}
	_ = store.IncrementSignal(context.Background(), "job-42", cached, 0)
	_ = store.IncrementSignal(context.Background(), "job-42", nil, 0)
	node.pollOnce(context.Background())
	records := host.emittedRecords()
	if len(records) != 1 || len(records[0].ports) != 1 || records[0].ports[0] != "output_1" {
		t.Fatalf("released message should emit on output_1: %+v", records)
	}
	got := records[0].msg.GetMetadata()
	if got["conflict"] != "waiting" || got["worker"] != "alpha" {
		t.Fatalf("wait-wins merge wrong: %#v", got)
	}
	if waitMeta, ok := nodebase.GetPath(got, "wait.signal"); !ok || waitMeta != "job-42" {
		t.Fatalf("release annotation missing: %#v", got)
	}
	if _, ok, _ := store.Signal(context.Background(), "job-42"); ok {
		t.Fatal("fully consumed signal row should be removed")
	}
}

func TestWaitNotifyWinsMerge(t *testing.T) {
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{"conflict": "waiting"})
	mergeCachedMetadata(msg, map[string]interface{}{"conflict": "notify"}, "notify-wins")
	if msg.GetMetadata()["conflict"] != "notify" {
		t.Fatalf("notify-wins should overwrite: %#v", msg.GetMetadata())
	}
}

func TestWaitPollRoutesExpiredToOutput2(t *testing.T) {
	store := newMemoryCoordinationStore()
	host := newFakeHost(store)
	defer host.cancel()
	node := waitTestNode(waitConfig(), host)
	expiredMessage := nodestore.NewMessage().SetPayload("late")
	err := store.ParkWaiter(context.Background(), nodebase.CoordinationWaiter{
		ID: "expired", SignalKey: "job-42", WorkflowID: "wf", NodeID: "w1", TargetCount: 1,
		Message: expiredMessage, ExpiresAt: time.Now().UTC().Add(-time.Second), CreatedAt: time.Now().UTC().Add(-2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	node.pollOnce(context.Background())
	records := host.emittedRecords()
	if len(records) != 1 || len(records[0].ports) != 1 || records[0].ports[0] != "output_2" {
		t.Fatalf("expired message should emit on output_2: %+v", records)
	}
	if expired, ok := nodebase.GetPath(records[0].msg.GetMetadata(), "wait.expired"); !ok || expired != true {
		t.Fatalf("expiration annotation missing: %#v", records[0].msg.GetMetadata())
	}
}

func TestWaitActivateStartsCancellablePoller(t *testing.T) {
	store := newMemoryCoordinationStore()
	host := newFakeHost(store)
	defer host.cancel()
	node := waitTestNode(waitConfig(), host)
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"id": "42", "count": 1})
	if _, err := node.Execute(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if err := node.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = store.IncrementSignal(context.Background(), "job-42", nil, 0)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(host.emittedRecords()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(host.emittedRecords()) != 1 {
		t.Fatal("activation poller did not release the waiter")
	}
	if err := node.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitValidation(t *testing.T) {
	node := waitTestNode(&Config{Signal: "job", TargetCount: "1", MergeMode: "bogus"}, &fakeHost{store: newMemoryCoordinationStore()})
	if err := node.Validate(); err == nil || !strings.Contains(err.Error(), "merge mode") {
		t.Fatalf("bad merge mode should fail, got %v", err)
	}
	node = waitTestNode(&Config{Signal: "job", TargetCount: "1", MergeMode: "wait-wins", PollIntervalMS: 1}, &fakeHost{store: newMemoryCoordinationStore()})
	if err := node.Validate(); err == nil || !strings.Contains(err.Error(), "poll interval") {
		t.Fatalf("short poll interval should fail, got %v", err)
	}
	node = waitTestNode(&Config{Signal: "job", TargetCount: "0", MergeMode: "wait-wins"}, &fakeHost{store: newMemoryCoordinationStore()})
	if err := node.Validate(); err == nil || !strings.Contains(err.Error(), "positive integer") {
		t.Fatalf("zero fixed target should fail validation, got %v", err)
	}
	if got := targetCountTemplate(float64(2)); got != "2" {
		t.Fatalf("decoded numeric target should become template text, got %q", got)
	}
}

func TestWaitFactoryAcceptsDecodedNumericTargetCount(t *testing.T) {
	n := nodestore.NewNode("w-numeric", "wait").SetWorkflowID("wf").SetData(map[string]interface{}{
		"signal": "job", "targetCount": float64(2), "mergeMode": "wait-wins", "pollIntervalMs": float64(20),
	})
	executor, err := factory(n, &fakeHost{store: newMemoryCoordinationStore()})
	if err != nil {
		t.Fatalf("numeric target count should decode through factory: %v", err)
	}
	waitNode, ok := executor.(*Node)
	if !ok {
		t.Fatalf("factory returned %T, want *Node", executor)
	}
	if got := targetCountTemplate(waitNode.cfg.TargetCount); got != "2" {
		t.Fatalf("decoded target count = %q, want 2", got)
	}
}

type partialErrorCoordinationStore struct {
	nodebase.CoordinationStore
	waiter nodebase.CoordinationWaiter
}

func (s *partialErrorCoordinationStore) PollWaiters(ctx context.Context, workflowID, nodeID string) ([]nodebase.CoordinationWaiter, []nodebase.CoordinationWaiter, error) {
	return []nodebase.CoordinationWaiter{s.waiter}, nil, errors.New("later poll failure")
}

func TestWaitPollEmitsClaimedRowsReturnedWithError(t *testing.T) {
	msg := nodestore.NewMessage().SetPayload("claimed")
	store := &partialErrorCoordinationStore{
		CoordinationStore: newMemoryCoordinationStore(),
		waiter: nodebase.CoordinationWaiter{
			ID: "claimed-row", SignalKey: "job", WorkflowID: "wf", NodeID: "w1", TargetCount: 1,
			Message: msg, CreatedAt: time.Now().UTC(),
		},
	}
	host := newFakeHost(store)
	defer host.cancel()
	node := waitTestNode(waitConfig(), host)
	node.pollOnce(context.Background())
	records := host.emittedRecords()
	if len(records) != 1 || records[0].msg != msg {
		t.Fatalf("claimed row returned with an error must still be emitted: %+v", records)
	}
}
