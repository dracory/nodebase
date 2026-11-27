package nodebase

import (
	"context"
	"fmt"
	"time"

	nodestore "github.com/dracory/nodestore"
)

// This file defines the runner-neutral coordination contract behind the
// wait and notify nodes. The contract is deliberately small: nodebase
// owns the semantics executors need, while runners provide the concrete
// atomic storage implementation (for example a SQL database shared by
// several runners). No part of this contract is process-local.

// CoordinationSignal is the observable state of one release signal.
type CoordinationSignal struct {
	// Key is the evaluated release signal identifier shared by wait and
	// notify nodes. Keys are global across workflows, matching NiFi-style
	// distributed signal identity: two wait nodes in different workflows
	// compete for the same key unless the configured key includes a
	// workflow/run-specific prefix. The nodes trim surrounding whitespace
	// after template evaluation; within that trimmed form keys are exact,
	// case-sensitive strings.
	Key string
	// Count is the number of unconsumed signal increments.
	Count int64
	// Metadata is the notification metadata cache. Later notify nodes
	// overwrite earlier values for the same key; unrelated keys merge.
	Metadata map[string]interface{}
}

// CoordinationWaiter is one durably parked message. Message contains the
// complete message envelope so a runner can release it after a restart.
type CoordinationWaiter struct {
	ID          string
	SignalKey   string
	WorkflowID  string
	NodeID      string
	TargetCount int64
	Message     nodestore.MessageInterface
	// ExpiresAt is zero when the waiter never expires. Expiration is
	// independent of signal expiry.
	ExpiresAt time.Time
	CreatedAt time.Time
	// CachedMetadata is populated on a successful release result with the
	// signal's notification metadata cache. It is not part of the parked
	// waiter identity and is empty while the waiter is still parked.
	CachedMetadata map[string]interface{}
	// ClaimToken is the ownership token assigned by PollWaiters. It is
	// empty while the row is merely parked/observed and non-empty for a
	// row returned to the claiming caller. AckWaiter/NackWaiter must use
	// this exact token so an expired claim later reclaimed by another
	// runner cannot be acknowledged by the stale owner.
	ClaimToken string
	// ClaimExpiresAt is the lease deadline for ClaimToken. Implementations
	// may reclaim the row after this time when the owner crashed before
	// ack/nack. The outcome already decided for the row (released or
	// expired, including consumed count and cached metadata) survives that
	// reclaim so retry does not consume the signal twice.
	ClaimExpiresAt time.Time
}

// CoordinationStore is the durable multi-runner signal/waiting store.
//
// Semantics required of implementations:
//   - IncrementSignal atomically increments Key by one. ttl <= 0 means the
//     signal does not expire; ttl > 0 extends/expires the whole unconsumed
//     signal count from the latest increment. Expiry cleanup may be
//     opportunistic but expired counts must never release a waiter. Expiry
//     is evaluated on runner UTC clocks rather than database-specific
//     datetime functions, so multi-runner deployments should keep clocks
//     synchronized.
//   - Counts are consumed atomically in target-sized chunks. When a count
//     exceeds a waiter's target, the remainder stays available for later
//     waiters. A zero remainder is removed.
//   - ParkWaiter persists the complete message envelope. Parking does not
//     hold a goroutine or runner traversal open.
//   - PollWaiters claims expired waiters and releases waiters whose signal
//     count can be consumed for the workflow/node, scanned in parked-row
//     creation order. Expiration and release claims must be atomic across
//     runners: one row is owned by
//     at most one caller at a time. A claim is a lease, not destruction:
//     returned rows remain recoverable until AckWaiter deletes them. The
//     first successful release claim consumes the target count and records
//     that outcome in the row; later lease-expiry reclaims reuse the stored
//     outcome and must not consume the count again.
//   - The claiming caller must attempt a durable downstream handoff. On
//     handoff success it calls AckWaiter; on failure it calls NackWaiter.
//     NackWaiter preserves the parked row and its already-decided outcome
//     but clears ownership so the row is immediately retried. If the owner
//     crashes before either call, implementations make the row claimable
//     again after ClaimExpiresAt. This is the at-least-once delivery
//     boundary: a crash or partial downstream failure may produce a
//     duplicate, but must not silently lose the parked message.
//   - If an implementation returns rows together with a later error, the
//     caller still owns those returned rows and must process them; they
//     have already been claimed.
//   - Implementations must use conditional SQL statements, database
//     transactions, or an equivalent atomic mechanism. Ordinary context
//     values and process-local maps do not satisfy this contract.
//
// Waiters compete for signals in creation order as scanned by a polling
// node. A waiter whose target cannot yet be satisfied does not prevent a
// later waiter for the same signal from being considered. Metadata merge
// conflicts between the cached notification metadata and the waiting
// message are resolved by the wait node's configured merge mode, not by
// the store.
type CoordinationStore interface {
	IncrementSignal(ctx context.Context, key string, metadata map[string]interface{}, ttl time.Duration) error
	ParkWaiter(ctx context.Context, waiter CoordinationWaiter) error
	PollWaiters(ctx context.Context, workflowID, nodeID string) (released []CoordinationWaiter, expired []CoordinationWaiter, err error)
	// AckWaiter deletes waiterID after its downstream handoff was durably
	// persisted. claimToken must match the ownership token returned by
	// PollWaiters; implementations must not delete a row owned by someone
	// else. Implementations may make repeat/missing-row acknowledgement
	// idempotent.
	AckWaiter(ctx context.Context, waiterID, claimToken string) error
	// NackWaiter clears the caller's ownership after a handoff failure so
	// the waiter can be claimed and retried immediately. It must preserve
	// the message and any already-decided release/expired outcome; in
	// particular it must never restore signal count or discard cached
	// release metadata. Implementations must not alter a row owned by
	// someone else.
	NackWaiter(ctx context.Context, waiterID, claimToken string) error
	Signal(ctx context.Context, key string) (CoordinationSignal, bool, error)
	Waiters(ctx context.Context, workflowID, nodeID string) ([]CoordinationWaiter, error)
}

// coordinationDurableEmitter is the optional structural host capability
// used at the release/expiry boundary. Delivering through ordinary Emit is
// intentionally not accepted because Emit is asynchronous and may return
// success before any downstream durable inbox row exists.
type coordinationDurableEmitter interface {
	EmitDurable(workflowID, nodeID string, payload nodestore.MessageInterface, ports []string) error
}

// ResolveCoordinationDurableEmitter returns a host durable emitter. The
// emission must synchronously traverse downstream and report persistence
// failures so the wait node can nack rather than lose the parked row.
func ResolveCoordinationDurableEmitter(host NodeHost) (func(workflowID, nodeID string, payload nodestore.MessageInterface, ports []string) error, error) {
	if host == nil {
		return nil, fmt.Errorf("coordination durable emitter unavailable: no node host")
	}
	emitter, ok := host.(coordinationDurableEmitter)
	if !ok {
		return nil, fmt.Errorf("coordination durable emitter unavailable: host does not provide EmitDurable")
	}
	// Hosts that expose the durable-inbox readiness probe (the reference
	// noderun host does) must actually have that store wired. Otherwise
	// deploying wait would only discover a permanent emission failure after
	// messages were parked.
	if readiness, ok := host.(interface{ HasDurableStore() bool }); ok && !readiness.HasDurableStore() {
		return nil, fmt.Errorf("coordination durable emitter unavailable: no durable inbox store configured")
	}
	return emitter.EmitDurable, nil
}

// coordinationStoreProvider is the optional structural host capability.
// It is deliberately not part of NodeHost itself: existing third-party
// hosts remain source-compatible, while runners offering distributed
// coordination expose this method.
type coordinationStoreProvider interface {
	ResolvedCoordinationStore() CoordinationStore
}

// ResolveCoordinationStore returns the host's durable coordination store
// or an explicit error. Wait/notify nodes fail validation or execution with
// this error rather than silently degrading to process-local memory.
func ResolveCoordinationStore(host NodeHost) (CoordinationStore, error) {
	if host == nil {
		return nil, fmt.Errorf("coordination store unavailable: no node host")
	}
	provider, ok := host.(coordinationStoreProvider)
	if !ok {
		return nil, fmt.Errorf("coordination store unavailable: host does not provide ResolvedCoordinationStore")
	}
	store := provider.ResolvedCoordinationStore()
	if store == nil {
		return nil, fmt.Errorf("coordination store unavailable: no durable store configured")
	}
	return store, nil
}
