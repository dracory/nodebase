package nodebase

// Pack-local unit tests for the queue/checkpoint executor shells in
// queue_node.go — validation rules, route parsing and the
// QueueContract seam — against a fake QueueService.

import (
	"context"
	"errors"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

// fakeQueue records contract calls.
type fakeQueue struct {
	armed      bool
	enqueued   []nodestore.MessageInterface
	stopped    bool
	enqueueErr error
}

func (q *fakeQueue) Arm(ctx context.Context)                    { q.armed = true }
func (q *fakeQueue) Enqueue(p nodestore.MessageInterface) error {
	if q.enqueueErr != nil {
		return q.enqueueErr
	}
	q.enqueued = append(q.enqueued, p)
	return nil
}
func (q *fakeQueue) Stop() { q.stopped = true }

// fakeQueueService hands out fakeQueue instances.
type fakeQueueService struct {
	durable bool
	queue   *fakeQueue
	err     error
	lastCP  bool
	routes  []string
}

func (s *fakeQueueService) EnsureQueue(node nodestore.NodeInterface, checkpoint bool, routes []string) (QueueContract, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.lastCP = checkpoint
	s.routes = routes
	if s.queue == nil {
		s.queue = &fakeQueue{}
	}
	return s.queue, nil
}
func (s *fakeQueueService) HasDurableStore() bool { return s.durable }

type queueHost struct {
	stubHost
	svc QueueService
}

func (h *queueHost) QueueService() QueueService { return h.svc }

func TestQueueExecuteStagesAndStops(t *testing.T) {
	svc := &fakeQueueService{durable: true}
	host := &queueHost{svc: svc}
	node := nodestore.NewNode("q1", "queue").SetWorkflowID("wf")
	n := NewQueueNode(node, &QueueConfig{Backend: "durable"}, host)

	if err := n.Activate(context.Background()); err != nil {
		t.Fatalf("activate failed: %v", err)
	}
	if !svc.queue.armed {
		t.Fatal("Activate must arm the drainer")
	}
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != nil {
		t.Fatal("staged message must not pass through")
	}
	if len(svc.queue.enqueued) != 1 {
		t.Fatal("message not staged")
	}
	if err := n.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if !svc.queue.stopped {
		t.Fatal("Stop must shut down the drainer")
	}
	if n.StopKey() == "" {
		t.Fatal("StopKey missing")
	}
}

func TestQueueValidate(t *testing.T) {
	node := nodestore.NewNode("q1", "queue").SetWorkflowID("wf")

	// durable backend needs a durable store
	host := &queueHost{svc: &fakeQueueService{durable: false}}
	if err := NewQueueNode(node, &QueueConfig{Backend: "durable"}, host).Validate(); err == nil {
		t.Fatal("durable backend without store should fail")
	}
	hostOK := &queueHost{svc: &fakeQueueService{durable: true}}
	if err := NewQueueNode(node, &QueueConfig{}, hostOK).Validate(); err != nil {
		t.Fatalf("default (durable) backend should pass: %v", err)
	}
	// dir backend needs a dir
	if err := NewQueueNode(node, &QueueConfig{Backend: "dir"}, hostOK).Validate(); err == nil {
		t.Fatal("dir backend without dir should fail")
	}
	if err := NewQueueNode(node, &QueueConfig{Backend: "dir", Dir: "/tmp/q"}, hostOK).Validate(); err != nil {
		t.Fatalf("dir backend should pass: %v", err)
	}
	// unknown backend
	if err := NewQueueNode(node, &QueueConfig{Backend: "bogus"}, hostOK).Validate(); err == nil {
		t.Fatal("unknown backend should fail")
	}
	// bad policy
	if err := NewQueueNode(node, &QueueConfig{Backend: "dir", Dir: "/x", Policy: "explode"}, hostOK).Validate(); err == nil {
		t.Fatal("unknown policy should fail")
	}
	if err := NewQueueNode(node, &QueueConfig{Backend: "dir", Dir: "/x", Policy: "drop-oldest"}, hostOK).Validate(); err != nil {
		t.Fatalf("valid policy should pass: %v", err)
	}
	// no queue service at all
	if err := NewQueueNode(node, &QueueConfig{}, &stubHost{}).Validate(); err == nil {
		t.Fatal("nil queue service should fail validation")
	}
}

func TestQueueExecuteErrors(t *testing.T) {
	svc := &fakeQueueService{durable: true, err: errors.New("no queue")}
	host := &queueHost{svc: svc}
	node := nodestore.NewNode("q1", "queue").SetWorkflowID("wf")
	n := NewQueueNode(node, &QueueConfig{Backend: "durable"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("queue creation failure should propagate")
	}
	svc.err = nil
	svc.queue = &fakeQueue{enqueueErr: errors.New("full")}
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("enqueue failure should propagate")
	}
}

func TestCheckpointNode(t *testing.T) {
	svc := &fakeQueueService{durable: true}
	host := &queueHost{svc: svc}
	node := nodestore.NewNode("c1", "checkpoint").SetWorkflowID("wf")
	cfg := &CheckpointConfig{
		QueueConfig: QueueConfig{Backend: "durable"},
		Routes:      "approve, decline",
	}
	n := NewCheckpointNode(node, cfg, host)
	if err := n.Activate(context.Background()); err != nil {
		t.Fatalf("activate failed: %v", err)
	}
	if !svc.lastCP {
		t.Fatal("checkpoint flag not passed to EnsureQueue")
	}
	if len(svc.routes) != 2 || svc.routes[0] != "approve" {
		t.Fatalf("routes wrong: %v", svc.routes)
	}
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("doc")); err != nil {
		t.Fatalf("stage failed: %v", err)
	}
	if len(svc.queue.enqueued) != 1 {
		t.Fatal("checkpoint item not staged")
	}
}

func TestParseCheckpointRoutes(t *testing.T) {
	r := ParseCheckpointRoutes("a, b ,c")
	if len(r) != 3 || r[1] != "b" {
		t.Fatalf("comma routes wrong: %v", r)
	}
	r = ParseCheckpointRoutes([]interface{}{"x", 42})
	if len(r) != 2 || r[0] != "x" || r[1] != "" {
		t.Fatalf("array routes wrong: %v", r)
	}
	r = ParseCheckpointRoutes([]string{"only"})
	if len(r) != 1 || r[0] != "only" {
		t.Fatalf("string-slice routes wrong: %v", r)
	}
	if ParseCheckpointRoutes(42) != nil {
		t.Fatal("non-list routes should be nil")
	}
}
