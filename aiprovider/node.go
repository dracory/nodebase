// Package aiprovider contains the ai-provider config node
// implementation: a shared LLM provider definition (provider, base
// URL, API key credential, default model and timeout) referenced by
// the ai-request node. The client layer is github.com/dracory/llm.
package aiprovider

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// Type is the registered node type id for the provider config entity.
const Type = "ai-provider"

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(Config)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewProviderNode(n, c), nil
}

// init registers the ai-provider node with the core set and marks the
// API key as a credential field (masked in the editor, moved out of
// the exported flow document).
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        Type,
			Name:        "AI Provider",
			Description: "Shared LLM provider definition referenced by ai-request nodes",
			Icon:        "cloud",
			Category:    "AI",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
	nodebase.RegisterCredentialKeys(Type, []string{"apiKey"})
}

// Providers offered by the config node — the set dracory/llm ships.
// "custom" covers any OpenAI-compatible endpoint (Ollama, llama.cpp,
// vLLM) via BaseURL; "mock" exists for tests and demos.
var providerIDs = []string{
	string(llm.ProviderOpenAI),
	string(llm.ProviderAnthropic),
	string(llm.ProviderGemini),
	string(llm.ProviderVertex),
	string(llm.ProviderOpenRouter),
	string(llm.ProviderCustom),
	string(llm.ProviderMock),
}

// schema is the JSON schema the editor serves for the ai-provider node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"provider": map[string]interface{}{
			"type":        "string",
			"title":       "Provider",
			"enum":        providerIDs,
			"default":     string(llm.ProviderCustom),
			"description": "LLM provider; \"custom\" targets any OpenAI-compatible endpoint (Ollama, llama.cpp, vLLM)",
		},
		"baseURL": map[string]interface{}{
			"type":        "string",
			"title":       "Base URL",
			"description": "Endpoint URL — required for provider \"custom\" (e.g. http://localhost:11434/v1/chat/completions)",
		},
		"apiKey": map[string]interface{}{
			"type":        "string",
			"title":       "API Key",
			"format":      "credential",
			"description": "API key (credential: masked, never exported); optional for endpoints that need no auth",
		},
		"model": map[string]interface{}{
			"type":        "string",
			"title":       "Default Model",
			"description": "Model used by referencing nodes unless the provider supplies a default",
		},
		"timeoutSeconds": map[string]interface{}{
			"type":        "number",
			"title":       "Timeout (s)",
			"description": "Default per-call timeout in seconds; the referencing node's timeout wins when set",
		},
		"providerOptions": map[string]interface{}{
			"type":                 "object",
			"title":                "Provider Options",
			"description":          "Provider-specific options merged into the request (escape hatch)",
			"additionalProperties": true,
		},
	},
	"required": []string{"provider"},
}

// Config holds the ai-provider configuration.
type Config struct {
	// Provider is the dracory/llm provider id: openai, anthropic,
	// gemini, vertex, openrouter, custom or mock.
	Provider string `json:"provider"`
	// BaseURL is the endpoint URL — required for provider "custom"
	// (any OpenAI-compatible server); ignored by hosted providers.
	BaseURL string `json:"baseURL"`
	// APIKey is the provider credential (masked in the editor,
	// stripped from exports). Optional for endpoints needing no auth.
	APIKey string `json:"apiKey"`
	// Model is the default model for referencing nodes.
	Model string `json:"model"`
	// TimeoutSeconds is the default per-call timeout; zero uses the
	// provider default.
	TimeoutSeconds float64 `json:"timeoutSeconds"`
	// ProviderOptions carries provider-specific options merged into
	// llm.LlmOptions.ProviderOptions. For provider "mock" the keys
	// "mock_response" (string) and "mock_error" (string) map onto
	// LlmOptions.MockResponse/MockError — the deterministic test seam.
	ProviderOptions map[string]interface{} `json:"providerOptions"`
}

// Options builds the llm.LlmOptions this provider implies — the
// base the referencing node merges its per-call overrides onto.
// The credential sentinel resolves to an empty key so a stored
// sentinel never reaches the wire.
func (c *Config) Options() llm.LlmOptions {
	opts := llm.LlmOptions{
		Provider: llm.Provider(c.Provider),
		ApiKey:   c.APIKey,
		Model:    c.Model,
	}
	if nodebase.IsCredentialSentinel(opts.ApiKey) {
		opts.ApiKey = ""
	}
	if c.TimeoutSeconds > 0 {
		opts.Timeout = time.Duration(c.TimeoutSeconds * float64(time.Second))
	}
	if len(c.ProviderOptions) > 0 {
		opts.ProviderOptions = make(map[string]any, len(c.ProviderOptions)+1)
		for k, v := range c.ProviderOptions {
			opts.ProviderOptions[k] = v
		}
	}
	if c.BaseURL != "" {
		if opts.ProviderOptions == nil {
			opts.ProviderOptions = map[string]any{}
		}
		opts.ProviderOptions["base_url"] = c.BaseURL
	}
	if opts.Provider == llm.ProviderMock {
		if s, ok := c.ProviderOptions["mock_response"].(string); ok {
			opts.MockResponse = s
		}
		if s, ok := c.ProviderOptions["mock_error"].(string); ok && s != "" {
			opts.MockError = errors.New(s)
		}
	}
	return opts
}

// ProviderNode is a config entity; its Execute is never invoked (no
// ports), existing only to satisfy the NodeExecutor contract.
type ProviderNode struct {
	*nodebase.BaseNode
	config *Config
}

// NewProviderNode creates a new ai-provider node.
func NewProviderNode(node nodestore.NodeInterface, config *Config) *ProviderNode {
	return &ProviderNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute passes the payload through — unreachable in practice since
// an ai-provider node has no inputs.
func (n *ProviderNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// Validate checks the structural config: a known provider id, and a
// base URL when provider is "custom".
func (n *ProviderNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	known := false
	for _, id := range providerIDs {
		if n.config.Provider == id {
			known = true
			break
		}
	}
	if !known {
		return fmt.Errorf("ai-provider: unknown provider %q", n.config.Provider)
	}
	if n.config.Provider == string(llm.ProviderCustom) && n.config.BaseURL == "" {
		return errors.New("ai-provider: baseURL is required for provider \"custom\"")
	}
	return nil
}

// ResolveOptions looks up an ai-provider config node reference inside
// the workflow and returns the llm.LlmOptions it implies — the shared
// first step for every node that calls a provider (ai-request,
// ai-agent). Per-call overrides are applied by the caller.
func ResolveOptions(host nodebase.NodeHost, workflowID, ref string) (llm.LlmOptions, error) {
	if host == nil {
		return llm.LlmOptions{}, fmt.Errorf("no host — cannot resolve ai-provider %q", ref)
	}
	providerNode, ok := host.LookupNode(workflowID, ref)
	if !ok {
		return llm.LlmOptions{}, fmt.Errorf("ai-provider %q not found in workflow", ref)
	}
	cfg, err := ConfigForNode(providerNode)
	if err != nil {
		return llm.LlmOptions{}, err
	}
	return cfg.Options(), nil
}

// ConfigForNode decodes the ai-provider config off a stored node
// definition — the path ai-request takes after LookupNode.
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
