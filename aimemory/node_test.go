package aimemory

// Pack-local unit tests for the ai-memory config entity: config
// decoding, conversation-key resolution and store-backed history
// with the window cap and retention.

import (
	"testing"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

func memoryNode(id string, data map[string]any) nodestore.NodeInterface {
	return nodestore.NewNode(id, Type).SetData(data)
}

func TestConfigForNode(t *testing.T) {
	cfg, err := ConfigForNode(memoryNode("m1", map[string]any{
		"keyProperty":      "msg.sessionId",
		"maxMessages":      10,
		"retentionSeconds": 300,
	}))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if cfg.KeyProperty != "msg.sessionId" || cfg.MaxMessages != 10 || cfg.RetentionSeconds != 300 {
		t.Fatalf("config wrong: %+v", cfg)
	}
	if _, err := ConfigForNode(nodestore.NewNode("d", "debug")); err == nil {
		t.Fatal("expected non-ai-memory error")
	}
}

func TestConversationKey(t *testing.T) {
	cfg := &Config{KeyProperty: "msg.conversationId"}
	msg := nodestore.NewMessage()
	msg.GetMetadata()["conversationId"] = "conv-1"
	if got := cfg.ConversationKey(msg); got != "conv-1" {
		t.Fatalf("key wrong: %q", got)
	}
	// Missing property falls back to "default".
	if got := cfg.ConversationKey(nodestore.NewMessage()); got != "default" {
		t.Fatalf("fallback wrong: %q", got)
	}
	// Payload paths work too.
	cfg2 := &Config{KeyProperty: "payload.user"}
	m2 := nodestore.NewMessage().SetPayload(map[string]interface{}{"user": "u1"})
	if got := cfg2.ConversationKey(m2); got != "u1" {
		t.Fatalf("payload key wrong: %q", got)
	}
}

func TestStoreRoundTripAndWindowCap(t *testing.T) {
	cs := nodebase.NewMemoryContextStore()
	cfg := &Config{MaxMessages: 3}
	s := OpenStore(cs, "m1", cfg, "c1")
	if s == nil {
		t.Fatal("store must open")
	}
	for i := 0; i < 5; i++ {
		if err := s.Append(llm.Message{Role: llm.RoleUser, Content: string(rune('a' + i))}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	msgs, err := s.Load()
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(msgs) != 3 || msgs[0].Content != "c" || msgs[2].Content != "e" {
		t.Fatalf("window cap wrong: %+v", msgs)
	}

	// A second store over the same backend sees the same history —
	// the "engine restart" path.
	s2 := OpenStore(cs, "m1", cfg, "c1")
	msgs2, _ := s2.Load()
	if len(msgs2) != 3 {
		t.Fatal("history did not persist across store rebind")
	}
	// Different conversation key = isolated history.
	s3 := OpenStore(cs, "m1", cfg, "c2")
	msgs3, _ := s3.Load()
	if len(msgs3) != 0 {
		t.Fatal("foreign conversation leaked history")
	}
}

func TestOpenStoreNilGuards(t *testing.T) {
	if OpenStore(nil, "m1", &Config{}, "c1") != nil {
		t.Fatal("nil context store must disable memory")
	}
	if OpenStore(nodebase.NewMemoryContextStore(), "m1", &Config{}, "") != nil {
		t.Fatal("empty key must disable memory")
	}
}
