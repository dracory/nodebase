package aiprovider

// Pack-local unit tests for the ai-provider config node — Options()
// mapping onto dracory/llm options, credential handling and
// validation. The Mock provider's Calls() introspection is asserted
// through llm.NewLLM — no live HTTP.

import (
	"context"
	"testing"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

func newProviderNode(cfg *Config) *ProviderNode {
	raw := map[string]any{
		"provider": cfg.Provider,
	}
	if cfg.BaseURL != "" {
		raw["baseURL"] = cfg.BaseURL
	}
	if cfg.APIKey != "" {
		raw["apiKey"] = cfg.APIKey
	}
	if cfg.Model != "" {
		raw["model"] = cfg.Model
	}
	n := nodestore.NewNode("p1", Type).SetData(raw)
	return NewProviderNode(n, cfg)
}

func TestValidate(t *testing.T) {
	if err := newProviderNode(&Config{Provider: "openai"}).Validate(); err != nil {
		t.Fatalf("openai should validate: %v", err)
	}
	if err := newProviderNode(&Config{Provider: "bogus"}).Validate(); err == nil {
		t.Fatal("expected unknown-provider error")
	}
	if err := newProviderNode(&Config{Provider: "custom"}).Validate(); err == nil {
		t.Fatal("expected missing-baseURL error for custom")
	}
	if err := newProviderNode(&Config{Provider: "custom", BaseURL: "http://x/v1/chat/completions"}).Validate(); err != nil {
		t.Fatalf("custom with baseURL should validate: %v", err)
	}
}

func TestOptionsMapsFields(t *testing.T) {
	cfg := &Config{
		Provider:       "custom",
		BaseURL:        "http://localhost:11434/v1/chat/completions",
		APIKey:         "secret",
		Model:          "llama3",
		TimeoutSeconds: 5,
	}
	opts := cfg.Options()
	if opts.Provider != llm.ProviderCustom {
		t.Fatalf("provider wrong: %v", opts.Provider)
	}
	if opts.ApiKey != "secret" || opts.Model != "llama3" {
		t.Fatalf("apiKey/model wrong: %+v", opts)
	}
	if got := opts.ProviderOptions["base_url"]; got != cfg.BaseURL {
		t.Fatalf("base_url wrong: %v", got)
	}
	if opts.Timeout != 5_000_000_000 {
		t.Fatalf("timeout wrong: %v", opts.Timeout)
	}
}

func TestOptionsStripsCredentialSentinel(t *testing.T) {
	opts := (&Config{Provider: "openai", APIKey: nodebase.CredentialSentinel}).Options()
	if opts.ApiKey != "" {
		t.Fatalf("sentinel must resolve to empty key, got %q", opts.ApiKey)
	}
}

func TestOptionsMockSeam(t *testing.T) {
	cfg := &Config{
		Provider: "mock",
		ProviderOptions: map[string]interface{}{
			"mock_response": "canned",
			"mock_error":    "boom",
		},
	}
	opts := cfg.Options()
	if opts.MockResponse != "canned" || opts.MockError == nil {
		t.Fatalf("mock seam wrong: %+v", opts)
	}
}

// TestMockProviderCalls exercises the full client construction a
// referencing node performs: Config.Options → llm.NewLLM → Mock
// provider, asserting MockInterface.Calls() records the call.
func TestMockProviderCalls(t *testing.T) {
	cfg := &Config{
		Provider: "mock",
		Model:    "mock-model",
		ProviderOptions: map[string]interface{}{
			"mock_response": "hello world",
		},
	}
	client, err := llm.NewLLM(cfg.Options())
	if err != nil {
		t.Fatalf("NewLLM failed: %v", err)
	}
	got, err := client.GenerateText("sys", "hi")
	if err != nil {
		t.Fatalf("GenerateText failed: %v", err)
	}
	if got != "hello world" {
		t.Fatalf("response wrong: %q", got)
	}
	mock, ok := client.(llm.MockInterface)
	if !ok {
		t.Fatal("client does not implement MockInterface")
	}
	calls := mock.Calls()
	if len(calls) != 1 || calls[0].SystemPrompt != "sys" || calls[0].UserPrompt != "hi" {
		t.Fatalf("calls wrong: %+v", calls)
	}
}

func TestConfigForNode(t *testing.T) {
	node := nodestore.NewNode("p1", Type).SetData(map[string]any{"provider": "openai"})
	if _, err := ConfigForNode(node); err != nil {
		t.Fatalf("ConfigForNode failed: %v", err)
	}
	other := nodestore.NewNode("x", "debug")
	if _, err := ConfigForNode(other); err == nil {
		t.Fatal("expected type-mismatch error")
	}
}

func TestExecutePassesThrough(t *testing.T) {
	n := newProviderNode(&Config{Provider: "mock"})
	msg := nodestore.NewMessage().SetPayload("x")
	out, err := n.Execute(context.Background(), msg)
	if err != nil || out != msg {
		t.Fatalf("config node must pass through: %v", err)
	}
}
