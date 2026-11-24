package nodebase

import (
	"time"

	nodestore "github.com/dracory/nodestore"
)

// This file holds the runtime observability contracts an admin surface
// (e.g. pkg/nodeadmin) is written against. They are engine-agnostic —
// like contracts.go, nothing here may depend on a concrete runner.
// noderunner's *Queue satisfies QueueInspector and its run journal is
// made of RunJournalEntry values, so any runner exposing these shapes
// can back the admin API without the admin importing it.

// RunJournalSize is the runner-agnostic bound for per-node execution
// journals: how many recent run records an engine keeps per
// (workflow, node) for the run-details endpoint.
const RunJournalSize = 20

// QueueItem is one queued entry as exposed to the inspection API.
type QueueItem struct {
	Seq        int64                      `json:"seq"`
	EnqueuedAt time.Time                  `json:"enqueued_at"`
	Payload    nodestore.MessageInterface `json:"-"`
}

// NodeEvent is a single timestamped entry in a workflow's execution
// log. Events feed the editor's debug panel and per-node status
// indicators; the workflow-status endpoint returns the ones recorded
// since a caller-supplied sequence number.
type NodeEvent struct {
	Seq      int64     `json:"seq"`
	Time     time.Time `json:"time"`
	NodeID   string    `json:"node_id,omitempty"`
	NodeName string    `json:"node_name,omitempty"`
	Level    string    `json:"level"` // info | warn | error | debug
	Message  string    `json:"message"`
}

// RunJournalEntry records one node execution: when it ran, how long it
// took, a summarized credential-safe copy of the input and output
// payloads, and the error text when the node failed.
type RunJournalEntry struct {
	Seq        int64       `json:"seq"`
	Time       time.Time   `json:"time"`
	DurationMs float64     `json:"duration_ms"`
	Input      interface{} `json:"input"`
	Output     interface{} `json:"output"`
	Error      string      `json:"error,omitempty"`
}

// QueueInspector is the read/control surface a runner exposes for a
// queue node's live staging queue: depth and item listing, the
// hold/release gate, and per-item delete/requeue (the same endpoint
// serves checkpoint route releases via ReleaseItem's route parameter).
type QueueInspector interface {
	// Items returns the queued entries in FIFO order.
	Items() []QueueItem
	// Count reports the queued depth (excluding any waitlist).
	Count() int
	// Held reports whether the queue is held (accepting, not draining).
	Held() bool
	// SetHold toggles the hold gate at runtime.
	SetHold(hold bool)
	// Enqueue stages a payload under the queue's capacity/policy.
	Enqueue(p nodestore.MessageInterface) error
	// DeleteItem drops the queued item with the given seq.
	DeleteItem(seq int64) error
	// RequeueItem releases one queued item downstream on all edges.
	RequeueItem(seq int64) error
	// ReleaseItem releases one queued item; route picks the outgoing
	// edge for checkpoint queues (empty = all edges), approvedBy is
	// recorded in the released payload's checkpoint metadata.
	ReleaseItem(seq int64, route string, approvedBy string) error
	// RouteLabels returns the checkpoint's named release routes in
	// port order; plain queues report nil.
	RouteLabels() []string
}
