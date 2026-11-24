package nodebase

import (
	"encoding/json"
	"sort"
	"sync"
)

// Context scopes — Node-RED parity. The scope ID namespaces entries
// within a scope: node scope uses "<workflowID>/<nodeID>", flow scope
// uses the workflow ID, and the global scope uses "" (shared by every
// flow).
const (
	ContextScopeNode   = "node"
	ContextScopeFlow   = "flow"
	ContextScopeGlobal = "global"
)

// ContextStore is the persistence contract behind the function node's
// context API (context.get/set = node scope, flow.get/set, global.get/set).
// It lives in flowrunner — like DurableInboxStore and LeaseStore —
// because the import direction is flowadmin/storage -> flowrunner.
// Implementations must tolerate values that are arbitrary goja exports
// (map[string]interface{}, []interface{}, float64, string, bool, nil);
// disk-backed stores serialize them as JSON.
type ContextStore interface {
	// GetContext returns the value stored under key in the given
	// scope; ok is false when the key has never been set.
	GetContext(scope, scopeID, key string) (value interface{}, ok bool, err error)
	// SetContext stores value under key in the given scope.
	SetContext(scope, scopeID, key string, value interface{}) error
	// DeleteContext removes a single key (no error when absent).
	DeleteContext(scope, scopeID, key string) error
	// DeleteContextScope removes every key in the scope
	// (e.g. when a workflow is deleted).
	DeleteContextScope(scope, scopeID string) error
	// ContextKeys lists the keys stored in the scope (Node-RED
	// context.keys() / flow.keys() parity).
	ContextKeys(scope, scopeID string) ([]string, error)
}

// contextClone normalizes a value through a JSON round-trip so the
// in-memory store exposes the same types a disk-backed store would
// after serialization (e.g. ints read back as float64), and so stored
// maps/slices cannot alias live script objects. Values that are not
// JSON-representable are returned unchanged.
func contextClone(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return v
	}
	return out
}

// memoryContextStore is the built-in ContextStore used when no
// persistent backend is configured. Three levels: scope → scopeID →
// key → value.
type memoryContextStore struct {
	mu   sync.Mutex
	data map[string]map[string]map[string]interface{}
}

// NewMemoryContextStore creates an empty in-memory context store.
func NewMemoryContextStore() *memoryContextStore {
	return &memoryContextStore{data: map[string]map[string]map[string]interface{}{}}
}

// GetContext implements ContextStore.
func (s *memoryContextStore) GetContext(scope, scopeID, key string) (interface{}, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[scope][scopeID][key]
	if !ok {
		return nil, false, nil
	}
	return contextClone(v), true, nil
}

// SetContext implements ContextStore.
func (s *memoryContextStore) SetContext(scope, scopeID, key string, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[scope] == nil {
		s.data[scope] = map[string]map[string]interface{}{}
	}
	if s.data[scope][scopeID] == nil {
		s.data[scope][scopeID] = map[string]interface{}{}
	}
	s.data[scope][scopeID][key] = contextClone(value)
	return nil
}

// DeleteContext implements ContextStore.
func (s *memoryContextStore) DeleteContext(scope, scopeID, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data[scope][scopeID], key)
	return nil
}

// DeleteContextScope implements ContextStore.
func (s *memoryContextStore) DeleteContextScope(scope, scopeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data[scope], scopeID)
	return nil
}

// ContextKeys implements ContextStore (sorted for determinism).
func (s *memoryContextStore) ContextKeys(scope, scopeID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.data[scope][scopeID]))
	for k := range s.data[scope][scopeID] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}
