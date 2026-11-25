// Package aimemory contains the ai-memory config node: a shared
// conversation-memory definition referenced by ai-agent nodes.
// History is kept in the host's context store (nodebase.ContextStore —
// persistent when the host wires a durable backend), keyed by a
// conversation id resolved from a message property. Retention and a
// context-window cap bound what is loaded; consolidation/long-term
// memory is explicitly deferred.
package aimemory

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// Type is the registered node type id for the memory config entity.
const Type = "ai-memory"

// scopeID prefixes the context-store scope so memory entries never
// collide with function-node context.
const scopePrefix = "ai-memory/"

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(Config)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewMemoryNode(n, c), nil
}

// init registers the ai-memory node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        Type,
			Name:        "AI Memory",
			Description: "Shared conversation memory referenced by ai-agent nodes",
			Icon:        "database",
			Category:    "AI",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the ai-memory node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"keyProperty": map[string]interface{}{
			"type":        "string",
			"title":       "Conversation Key",
			"default":     "msg.conversationId",
			"description": "Message property holding the conversation id (\"msg.x\" metadata or a payload path); empty/unset falls back to \"default\"",
		},
		"maxMessages": map[string]interface{}{
			"type":        "integer",
			"title":       "Context Window",
			"default":     40,
			"description": "Maximum number of stored turns loaded into the prompt — the oldest are dropped first",
		},
		"retentionSeconds": map[string]interface{}{
			"type":        "number",
			"title":       "Retention (s)",
			"description": "Forget turns older than this many seconds; 0 keeps them until the window cap evicts them",
		},
	},
}

// Config holds the ai-memory configuration.
type Config struct {
	// KeyProperty is the message property holding the conversation
	// id: "msg.x" reads metadata, "payload"/"payload.x" reads the
	// payload. Empty or unresolved falls back to "default".
	KeyProperty string `json:"keyProperty"`
	// MaxMessages caps the stored turns loaded into the prompt
	// (oldest dropped first); <= 0 uses the default of 40.
	MaxMessages int `json:"maxMessages"`
	// RetentionSeconds forgets turns older than the bound;
	// <= 0 means no time-based eviction.
	RetentionSeconds float64 `json:"retentionSeconds"`
}

// MemoryNode is a config entity; its Execute is never invoked (no
// ports), existing only to satisfy the NodeExecutor contract.
type MemoryNode struct {
	*nodebase.BaseNode
	config *Config
}

// NewMemoryNode creates a new ai-memory node.
func NewMemoryNode(node nodestore.NodeInterface, config *Config) *MemoryNode {
	return &MemoryNode{BaseNode: nodebase.NewBaseNode(node), config: config}
}

// Execute passes the payload through — unreachable in practice since
// an ai-memory node has no inputs.
func (n *MemoryNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// Validate checks the structural config — every field is optional.
func (n *MemoryNode) Validate() error {
	return n.BaseNode.Validate()
}

// ConfigForNode decodes the ai-memory config off a stored node
// definition — the path ai-agent takes after LookupNode.
func ConfigForNode(node nodestore.NodeInterface) (*Config, error) {
	if node == nil || node.GetType() != Type {
		return nil, fmt.Errorf("node is not an %s config node", Type)
	}
	c := new(Config)
	if err := nodebase.DecodeConfig(node, c); err != nil {
		return nil, err
	}
	return c, nil
}

// ScopeID is the context-store scope the config node's memories live
// under (global scope — shared by every runner of the same store).
func ScopeID(nodeID string) string {
	return scopePrefix + nodeID
}

// window returns the effective context-window cap.
func (c *Config) window() int {
	if c.MaxMessages > 0 {
		return c.MaxMessages
	}
	return 40
}

// ConversationKey resolves the conversation id for a message: the
// configured property ("msg.x" metadata or a payload path), falling
// back to "default" when unset or unresolvable.
func (c *Config) ConversationKey(input nodestore.MessageInterface) string {
	prop := c.KeyProperty
	if prop == "" {
		prop = "msg.conversationId"
	}
	var v interface{}
	var ok bool
	if p, isMsg := nodebase.MsgPath(prop); isMsg {
		if input.GetMetadata() != nil {
			v, ok = nodebase.GetPath(input.GetMetadata(), p)
		}
	} else {
		// A leading "payload." targets a path inside the payload;
		// LookupProperty walks the payload map directly.
		prop = strings.TrimPrefix(prop, "payload.")
		v, ok = nodebase.LookupProperty(input, prop)
	}
	if !ok {
		return "default"
	}
	if s := nodebase.StringOf(v); s != "" {
		return s
	}
	return "default"
}

// entry is one stored turn: the llm.Message plus the unix time it was
// recorded (drives retention eviction).
type entry struct {
	T       float64     `json:"t"`
	Message llm.Message `json:"m"`
}

// Store is the per-conversation history bound to a context store —
// ai-agent loads it before the first model call and appends the turns
// the loop produced.
type Store struct {
	store nodebase.ContextStore
	scope string
	key   string
	cfg   *Config
}

// OpenStore binds the conversation identified by key on the host's
// resolved context store. store may be nil — the caller treats memory
// as disabled then (host without a context seam).
func OpenStore(store nodebase.ContextStore, memoryNodeID string, cfg *Config, key string) *Store {
	if store == nil || key == "" {
		return nil
	}
	return &Store{store: store, scope: ScopeID(memoryNodeID), key: key, cfg: cfg}
}

// Load returns the stored turns within the retention bound and the
// context-window cap, oldest first — ready for LlmOptions.Messages.
func (s *Store) Load() ([]llm.Message, error) {
	raw, ok, err := s.store.GetContext(nodebase.ContextScopeGlobal, s.scope, s.key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	entries := decodeEntries(raw)
	cutoff := time.Now().Unix()
	if s.cfg.RetentionSeconds > 0 {
		kept := entries[:0]
		for _, e := range entries {
			if float64(cutoff)-e.T <= s.cfg.RetentionSeconds {
				kept = append(kept, e)
			}
		}
		entries = kept
	}
	if w := s.cfg.window(); len(entries) > w {
		entries = entries[len(entries)-w:]
	}
	msgs := make([]llm.Message, 0, len(entries))
	for _, e := range entries {
		msgs = append(msgs, e.Message)
	}
	return msgs, nil
}

// Append records new turns and persists the history, applying
// retention and the window cap before writing.
func (s *Store) Append(messages ...llm.Message) error {
	raw, ok, err := s.store.GetContext(nodebase.ContextScopeGlobal, s.scope, s.key)
	if err != nil {
		return err
	}
	var entries []entry
	if ok {
		entries = decodeEntries(raw)
	}
	now := float64(time.Now().Unix())
	for _, m := range messages {
		if m.Role == "" {
			continue
		}
		entries = append(entries, entry{T: now, Message: m})
	}
	if w := s.cfg.window(); len(entries) > w {
		entries = entries[len(entries)-w:]
	}
	return s.store.SetContext(nodebase.ContextScopeGlobal, s.scope, s.key, entries)
}

// decodeEntries normalizes a stored value ([]entry after a JSON
// round-trip through the context store) back to typed entries.
// Malformed content yields an empty history rather than an error.
func decodeEntries(raw interface{}) []entry {
	buf, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var entries []entry
	if err := json.Unmarshal(buf, &entries); err != nil {
		return nil
	}
	return entries
}
