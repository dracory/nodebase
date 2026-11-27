package notify

import (
	"context"
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
	return nil, nil, nil
}

func (s *memoryCoordinationStore) Signal(ctx context.Context, key string) (nodebase.CoordinationSignal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	signal, ok := s.signals[key]
	return signal, ok, nil
}

func (s *memoryCoordinationStore) Waiters(ctx context.Context, workflowID, nodeID string) ([]nodebase.CoordinationWaiter, error) {
	return nil, nil
}

type fakeHost struct {
	nodebase.NodeHost
	store nodebase.CoordinationStore
}

func (h *fakeHost) ResolvedCoordinationStore() nodebase.CoordinationStore { return h.store }

func notifyTestNode(cfg *Config, host nodebase.NodeHost) *Node {
	return NewNode(nodestore.NewNode("n1", "notify").SetWorkflowID("wf"), cfg, host)
}

func TestNotifyIncrementsTemplateSignalAndCachesMetadata(t *testing.T) {
	store := newMemoryCoordinationStore()
	host := &fakeHost{store: store}
	node := notifyTestNode(&Config{
		Signal:           "job-{{payload.id}}",
		CacheKeys:        "worker, msg.stats.count, missing",
		SignalTTLSeconds: 30,
	}, host)
	msg := nodestore.NewMessage().SetPayload(map[string]interface{}{"id": "42"}).SetMetadata(map[string]interface{}{
		"worker": "alpha",
		"stats":  map[string]interface{}{"count": 3},
	})
	out, err := node.Execute(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if out != msg {
		t.Fatal("successful notify must pass the message through")
	}
	signal, ok, err := store.Signal(context.Background(), "job-42")
	if err != nil || !ok {
		t.Fatalf("signal missing: ok=%v err=%v", ok, err)
	}
	if signal.Count != 1 {
		t.Fatalf("count = %d, want 1", signal.Count)
	}
	if signal.Metadata["worker"] != "alpha" || signal.Metadata["stats.count"] != 3 {
		t.Fatalf("cached metadata wrong: %#v", signal.Metadata)
	}
	if _, exists := signal.Metadata["missing"]; exists {
		t.Fatal("missing metadata keys must be ignored")
	}
}

func TestNotifyBlankEvaluatedSignalFails(t *testing.T) {
	store := newMemoryCoordinationStore()
	node := notifyTestNode(&Config{Signal: "{{payload.missing}}"}, &fakeHost{store: store})
	_, err := node.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{}))
	if err == nil || !strings.Contains(err.Error(), "blank") {
		t.Fatalf("blank evaluated signal should fail, got %v", err)
	}
}

func TestNotifyRequiresDurableStore(t *testing.T) {
	node := notifyTestNode(&Config{Signal: "job"}, &fakeHost{})
	if err := node.Validate(); err == nil || !strings.Contains(err.Error(), "coordination store unavailable") {
		t.Fatalf("validate should require coordination store, got %v", err)
	}
	_, err := node.Execute(context.Background(), nodestore.NewMessage())
	if err == nil || !strings.Contains(err.Error(), "coordination store unavailable") {
		t.Fatalf("execute should require coordination store, got %v", err)
	}
}

func TestNotifyValidation(t *testing.T) {
	node := notifyTestNode(&Config{Signal: "job", SignalTTLSeconds: -1}, &fakeHost{store: newMemoryCoordinationStore()})
	if err := node.Validate(); err == nil || !strings.Contains(err.Error(), "TTL") {
		t.Fatalf("negative TTL should fail, got %v", err)
	}
}
