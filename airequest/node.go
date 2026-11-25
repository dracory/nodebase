// Package airequest contains the ai-request node implementation:
// renders a mustache user-prompt template against the incoming
// message, calls an LLM through github.com/dracory/llm using an
// ai-provider config node, parses the response per the parse mode and
// writes it to a configurable message property.
package airequest

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	"github.com/dracory/nodebase/aiprovider"
	"github.com/dracory/nodebase/template"
	nodestore "github.com/dracory/nodestore"
	"gopkg.in/yaml.v3"
)

//go:embed editor.html
var editorHTML string

// newClient builds the LLM client — a variable so tests can capture
// the instance the node creates (MockInterface assertions).
var newClient = llm.NewLLM

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(Config)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewRequestNode(n, c, e), nil
}

// init registers the ai-request node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "ai-request",
			Name:        "AI Request",
			Description: "Prompts an LLM via an ai-provider config and writes the parsed reply to a msg property",
			Icon:        "comment",
			Category:    "AI",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the ai-request node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"provider": map[string]interface{}{
			"type":        "string",
			"title":       "AI Provider",
			"description": "ID of the ai-provider config node to call through",
		},
		"systemPrompt": map[string]interface{}{
			"type":        "string",
			"title":       "System Prompt",
			"description": "Optional system prompt sent verbatim",
		},
		"prompt": map[string]interface{}{
			"type":        "string",
			"title":       "User Prompt",
			"description": "Mustache template rendered against the message ({{payload.x}}, {{env.X}}, {{flow.X}}, {{global.X}})",
		},
		"parseMode": map[string]interface{}{
			"type":        "string",
			"title":       "Parse",
			"enum":        []string{"text", "json", "yaml"},
			"default":     "text",
			"description": "How the response text is interpreted before it is written to the target",
		},
		"target": map[string]interface{}{
			"type":        "string",
			"title":       "Target",
			"default":     "payload",
			"description": "Message property the result is written to: \"payload\" or a (dotted) msg property such as msg.llm.answer",
		},
		"temperature": map[string]interface{}{
			"type":        "number",
			"title":       "Temperature",
			"description": "Sampling temperature; empty uses the provider default",
		},
		"timeoutSeconds": map[string]interface{}{
			"type":        "number",
			"title":       "Timeout (s)",
			"description": "Per-call timeout in seconds; overrides the provider default when set",
		},
		"maxTokens": map[string]interface{}{
			"type":        "integer",
			"title":       "Max Tokens",
			"description": "Response token cap; empty uses the provider default",
		},
		"concurrency": map[string]interface{}{
			"type":        "integer",
			"title":       "Concurrency",
			"description": "Maximum in-flight LLM calls; additional messages wait. Empty/0 means unlimited",
		},
	},
	"required": []string{"provider"},
}

// Config holds the ai-request configuration.
type Config struct {
	// Provider is the ID of the ai-provider config node to call through.
	Provider string `json:"provider"`
	// SystemPrompt is sent verbatim as the system message.
	SystemPrompt string `json:"systemPrompt"`
	// Prompt is the mustache template rendered against the message
	// (same machinery as the template node: payload + metadata top
	// level, {{env.X}}/{{flow.X}}/{{global.X}} context tags).
	Prompt string `json:"prompt"`
	// ParseMode: "text" (default), "json" or "yaml" — how the response
	// text is decoded before write-back.
	ParseMode string `json:"parseMode"`
	// Target is the message property the result is written to:
	// "payload" (default), "msg.foo" or a bare metadata path.
	Target string `json:"target"`
	// Temperature overrides the provider default when non-nil.
	Temperature *float64 `json:"temperature"`
	// TimeoutSeconds overrides the provider timeout when > 0.
	TimeoutSeconds float64 `json:"timeoutSeconds"`
	// MaxTokens overrides the provider default when > 0.
	MaxTokens int `json:"maxTokens"`
	// Concurrency caps in-flight LLM calls; <= 0 means unlimited.
	Concurrency int `json:"concurrency"`
}

// RequestNode is the ai-request executor.
type RequestNode struct {
	*nodebase.BaseNode
	host       nodebase.NodeHost
	workflowID string
	config     *Config

	// semMu and sem are the nil-host fallback for the concurrency
	// semaphore (unit tests); hosted runs keep the semaphore in the
	// per-run node state so it is shared across rebuilt executors.
	semMu sync.Mutex
	sem   chan struct{}
}

// NewRequestNode creates a new ai-request node. host may be nil —
// provider resolution and context tags then fail/resolve empty.
func NewRequestNode(node nodestore.NodeInterface, config *Config, host nodebase.NodeHost) *RequestNode {
	return &RequestNode{
		BaseNode:   nodebase.NewBaseNode(node),
		host:       host,
		workflowID: node.GetWorkflowID(),
		config:     config,
	}
}

// Execute renders the prompt, calls the provider and writes the
// parsed response to the target property. Errors return through the
// node's error path so catch nodes can handle them.
func (n *RequestNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	release, err := n.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	userPrompt, err := template.RenderMustache(n.config.Prompt, input, n.host, n.workflowID, n.GetID(), false)
	if err != nil {
		return nil, err
	}

	opts, cancel, err := n.providerOptions(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	n.event("info", fmt.Sprintf("calling LLM (provider=%s model=%s)", opts.Provider, opts.Model))
	client, err := newClient(opts)
	if err != nil {
		n.event("error", fmt.Sprintf("client setup failed: %v", err))
		return nil, fmt.Errorf("ai-request: %w", err)
	}

	raw, err := n.generate(client, userPrompt, opts)
	if err != nil {
		n.event("error", fmt.Sprintf("call failed: %v", err))
		return nil, fmt.Errorf("ai-request: %w", err)
	}

	value, err := n.parse(raw)
	if err != nil {
		n.event("error", err.Error())
		return nil, err
	}

	n.writeTarget(input, value)
	n.event("info", "response received")
	return input, nil
}

// generate issues the call: JSON parse mode goes through GenerateJSON
// (providers get the structured-output hint); text and yaml use
// GenerateText.
func (n *RequestNode) generate(client llm.LlmInterface, userPrompt string, opts llm.LlmOptions) (string, error) {
	if n.parseMode() == "json" {
		return client.GenerateJSON(n.config.SystemPrompt, userPrompt, opts)
	}
	return client.GenerateText(n.config.SystemPrompt, userPrompt, opts)
}

// parse decodes the response per the parse mode; malformed output is
// an error routed to the node's error path.
func (n *RequestNode) parse(raw string) (interface{}, error) {
	switch n.parseMode() {
	case "json":
		var data interface{}
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			return nil, fmt.Errorf("ai-request: response is not valid JSON: %w", err)
		}
		return data, nil
	case "yaml":
		var data interface{}
		if err := yaml.Unmarshal([]byte(raw), &data); err != nil {
			return nil, fmt.Errorf("ai-request: response is not valid YAML: %w", err)
		}
		return data, nil
	}
	return raw, nil
}

// parseMode resolves the effective parse mode (default text).
func (n *RequestNode) parseMode() string {
	switch n.config.ParseMode {
	case "json", "yaml":
		return n.config.ParseMode
	}
	return "text"
}

// providerOptions resolves the referenced ai-provider config node and
// merges the per-call overrides (context, timeout, temperature, max
// tokens). The returned cancel releases the call-scoped timeout
// context (nil-cancel is a no-op) — callers defer it so the deadline
// timer never outlives the call. Cancellation flows through
// opts.Context into the provider's request.
func (n *RequestNode) providerOptions(ctx context.Context) (llm.LlmOptions, context.CancelFunc, error) {
	noop := context.CancelFunc(func() {})
	if n.host == nil {
		return llm.LlmOptions{}, noop, fmt.Errorf("ai-request: no host — cannot resolve ai-provider %q", n.config.Provider)
	}
	providerNode, ok := n.host.LookupNode(n.workflowID, n.config.Provider)
	if !ok {
		return llm.LlmOptions{}, noop, fmt.Errorf("ai-request: ai-provider %q not found in workflow", n.config.Provider)
	}
	cfg, err := aiprovider.ConfigForNode(providerNode)
	if err != nil {
		return llm.LlmOptions{}, noop, err
	}

	opts := cfg.Options()
	if n.config.Temperature != nil {
		opts.Temperature = n.config.Temperature
	}
	if n.config.MaxTokens > 0 {
		opts.MaxTokens = n.config.MaxTokens
	}
	if n.config.TimeoutSeconds > 0 {
		opts.Timeout = time.Duration(n.config.TimeoutSeconds * float64(time.Second))
	}
	cancel := noop
	if opts.Timeout > 0 {
		var callCtx context.Context
		callCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		opts.Context = callCtx
	} else {
		opts.Context = ctx
	}
	return opts, cancel, nil
}

// acquire takes a slot on the concurrency semaphore (when a positive
// cap is configured), blocking until one frees or ctx is cancelled.
func (n *RequestNode) acquire(ctx context.Context) (func(), error) {
	sem := n.semaphore()
	if sem == nil {
		return func() {}, nil
	}
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// semaphore returns the in-flight call cap channel, or nil when the
// node is uncapped. Hosted runs keep it in per-run node state; the
// nil-host path lazily builds one on the node.
func (n *RequestNode) semaphore() chan struct{} {
	if n.config.Concurrency <= 0 {
		return nil
	}
	limit := n.config.Concurrency
	if n.host != nil {
		return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
			return make(chan struct{}, limit)
		}).(chan struct{})
	}
	n.semMu.Lock()
	defer n.semMu.Unlock()
	if n.sem == nil {
		n.sem = make(chan struct{}, limit)
	}
	return n.sem
}

// writeTarget stores the parsed value: "payload" goes to the payload;
// "msg.x" and bare paths go into message metadata (same convention as
// the template node's msg scope).
func (n *RequestNode) writeTarget(input nodestore.MessageInterface, value interface{}) {
	target := n.config.Target
	if target == "" {
		target = "payload"
	}
	if target == "payload" {
		input.SetPayload(value)
		return
	}
	if input.GetMetadata() == nil {
		input.SetMetadata(map[string]interface{}{})
	}
	if p, ok := nodebase.MsgPath(target); ok {
		nodebase.SetPath(input.GetMetadata(), p, value)
		return
	}
	nodebase.SetPath(input.GetMetadata(), target, value)
}

// event logs a status event through the host (status badge/debug
// panel). Prompts and keys are never logged — only call metadata.
func (n *RequestNode) event(level, message string) {
	if n.host == nil {
		return
	}
	n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), level, message)
}

// Validate checks the structural config: a provider reference is
// required and the parse mode must be a known value.
func (n *RequestNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Provider == "" {
		return fmt.Errorf("ai-request: provider reference is required")
	}
	switch n.config.ParseMode {
	case "", "text", "json", "yaml":
	default:
		return fmt.Errorf("ai-request: unknown parse mode %q", n.config.ParseMode)
	}
	return nil
}
