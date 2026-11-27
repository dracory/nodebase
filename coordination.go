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
	// notify nodes. Keys are exact, case-sensitive strings.
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
//   - PollWaiters first claims expired waiters for the workflow/node and
//     then releases waiters whose signal count can be consumed. Expiration
//     and release claims must be atomic across runners: one row is returned
//     to at most one caller. Returned expired rows are removed from the
//     parked set; returned released rows are removed and their target count
//     has already been consumed. If an implementation returns rows together
//     with a later error, the caller still owns those returned rows and must
//     process them; they have already been claimed.
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
	Signal(ctx context.Context, key string) (CoordinationSignal, bool, error)
	Waiters(ctx context.Context, workflowID, nodeID string) ([]CoordinationWaiter, error)
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
