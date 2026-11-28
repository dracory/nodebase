// Package aiagent contains the ai-agent node implementation: runs the
// model ↔ tool-call loop through github.com/dracory/llm's
// ToolsInterface.GenerateWithTools — the caller (this node) executes
// each requested tool by injecting a call message into an ai-tool
// node's flow and awaiting its ai-tool-return. Output 1 carries the
// final answer, output 2 the tool-call trace. Iterations are capped
// and every model/tool call runs under a timeout; conversation
// history optionally round-trips through an ai-memory config node.
package aiagent

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	"github.com/dracory/nodebase/aimemory"
	"github.com/dracory/nodebase/aiprovider"
	"github.com/dracory/nodebase/aitool"
	"github.com/dracory/nodebase/template"
	nodestore "github.com/dracory/nodestore"
)

// Type is the registered node type id.
const Type = "ai-agent"

//go:embed editor.html
var editorHTML string

// newClient builds the LLM client — a variable so tests can capture or
// substitute the instance the node creates.
var newClient = llm.NewLLM

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(Config)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewAgentNode(n, c, e), nil
}

// init registers the ai-agent node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        Type,
			Name:        "AI Agent",
			Description: "Runs the model ↔ tool-call loop: output 1 = final answer, output 2 = tool-call trace",
			Icon:        "user-md",
			Category:    "AI",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the ai-agent node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"provider": map[string]interface{}{
			"type":        "string",
			"title":       "AI Provider",
			"description": "ID of the ai-provider config node to call through",
		},
		"memory": map[string]interface{}{
			"type":        "string",
			"title":       "AI Memory",
			"description": "Optional ID of an ai-memory config node supplying conversation history",
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
		"tools": map[string]interface{}{
			"type":        "string",
			"title":       "Tools",
			"description": "Comma-separated ai-tool references: \"nodeID\" (same workflow) or \"workflowID/nodeID\"",
		},
		"maxIterations": map[string]interface{}{
			"type":        "integer",
			"title":       "Max Iterations",
			"default":     8,
			"description": "Mandatory cap on model ↔ tool rounds; exceeding it routes an error to the catch path",
		},
		"timeoutSeconds": map[string]interface{}{
			"type":        "number",
			"title":       "Model Timeout (s)",
			"description": "Per-call timeout for each model round; overrides the provider default when set",
		},
		"toolTimeoutSeconds": map[string]interface{}{
			"type":        "number",
			"title":       "Tool Timeout (s)",
			"default":     60,
			"description": "How long each tool call may run before its result is reported back as an error",
		},
		"target": map[string]interface{}{
			"type":        "string",
			"title":       "Target",
			"default":     "payload",
			"description": "Message property the final answer is written to",
		},
		"concurrency": map[string]interface{}{
			"type":        "integer",
			"title":       "Concurrency",
			"description": "Maximum in-flight agent runs; additional messages wait. Empty/0 means unlimited",
		},
	},
	"required": []string{"provider"},
}

// Config holds the ai-agent configuration.
type Config struct {
	// Provider is the ID of the ai-provider config node to call through.
	Provider string `json:"provider"`
	// Memory is the optional ID of an ai-memory config node.
	Memory string `json:"memory"`
	// SystemPrompt is sent verbatim as the system message.
	SystemPrompt string `json:"systemPrompt"`
	// Prompt is the mustache template rendered against the message.
	Prompt string `json:"prompt"`
	// Tools is a comma-separated list of ai-tool references:
	// "nodeID" within the same workflow or "workflowID/nodeID" for a
	// tool flow living in another running workflow.
	Tools string `json:"tools"`
	// MaxIterations caps model ↔ tool rounds (default 8).
	MaxIterations int `json:"maxIterations"`
	// TimeoutSeconds is the per-model-call timeout; overrides the
	// provider default when > 0.
	TimeoutSeconds float64 `json:"timeoutSeconds"`
	// ToolTimeoutSeconds bounds each tool invocation (default 60).
	ToolTimeoutSeconds float64 `json:"toolTimeoutSeconds"`
	// Target is the message property the final answer is written to.
	Target string `json:"target"`
	// Concurrency caps in-flight agent runs; <= 0 means unlimited.
	Concurrency int `json:"concurrency"`
}

// toolTarget is a resolved tool reference: the llm declaration plus
// the workflow/node an invocation is injected at.
type toolTarget struct {
	workflowID string
	nodeID     string
	tool       llm.Tool
}

// AgentNode is the ai-agent executor.
type AgentNode struct {
	*nodebase.BaseNode
	host       nodebase.NodeHost
	workflowID string
	config     *Config

	// semMu and sem are the nil-host fallback for the concurrency
	// semaphore (unit tests); hosted runs keep it in node state.
	semMu sync.Mutex
	sem   chan struct{}
}

// NewAgentNode creates a new ai-agent node. host may be nil —
// provider/tool resolution then fails with explicit errors.
func NewAgentNode(node nodestore.NodeInterface, config *Config, host nodebase.NodeHost) *AgentNode {
	return &AgentNode{
		BaseNode:   nodebase.NewBaseNode(node),
		host:       host,
		workflowID: node.GetWorkflowID(),
		config:     config,
	}
}

// Execute runs the agent loop and returns the message carrying the
// final answer (routed to output 1 by SelectPorts); the tool-call
// trace is emitted on output 2.
func (n *AgentNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	release, err := n.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	userPrompt, err := template.RenderMustache(n.config.Prompt, input, n.host, n.workflowID, n.GetID(), false)
	if err != nil {
		return nil, err
	}

	opts, err := n.options()
	if err != nil {
		return nil, err
	}

	tools, err := n.resolveTools()
	if err != nil {
		return nil, err
	}
	for _, t := range tools {
		opts.Tools = append(opts.Tools, t.tool)
	}

	client, err := newClient(opts)
	if err != nil {
		n.event("error", fmt.Sprintf("client setup failed: %v", err))
		return nil, fmt.Errorf("ai-agent: %w", err)
	}
	toolsClient, ok := client.(llm.ToolsInterface)
	if !ok {
		return nil, fmt.Errorf("ai-agent: provider %q does not support tool calling", opts.Provider)
	}

	memory, err := n.openMemory(input)
	if err != nil {
		return nil, err
	}
	history := []llm.Message{}
	if memory != nil {
		if history, err = memory.Load(); err != nil {
			return nil, fmt.Errorf("ai-agent: memory load failed: %w", err)
		}
	}

	trace := []interface{}{}
	messages := append([]llm.Message{}, history...)
	maxIter := n.config.MaxIterations
	if maxIter <= 0 {
		maxIter = 8
	}

	var answer string
	for i := 0; i < maxIter; i++ {
		callOpts := opts
		callOpts.Messages = messages
		callCtx, cancel := n.callContext(ctx)
		callOpts.Context = callCtx
		prompt := userPrompt
		if i > 0 {
			prompt = "" // continuation: messages carry the turns
		}
		n.event("info", fmt.Sprintf("model round %d/%d", i+1, maxIter))
		resp, err := toolsClient.GenerateWithTools(n.config.SystemPrompt, prompt, callOpts)
		cancel()
		if err != nil {
			n.event("error", fmt.Sprintf("model call failed: %v", err))
			n.emitTrace(input, trace)
			return nil, fmt.Errorf("ai-agent: %w", err)
		}
		messages = append(messages, resp.Message)

		if len(resp.ToolCalls) == 0 {
			answer = resp.Content
			trace = append(trace, map[string]interface{}{
				"iteration": i + 1, "kind": "answer", "content": resp.Content,
			})
			break
		}

		for _, call := range resp.ToolCalls {
			trace = append(trace, map[string]interface{}{
				"iteration": i + 1, "kind": "tool_call", "id": call.ID,
				"name": call.Name, "arguments": call.Arguments,
			})
			result, callErr := n.invokeTool(ctx, tools, call)
			entry := map[string]interface{}{
				"iteration": i + 1, "kind": "tool_result", "id": call.ID, "name": call.Name,
			}
			if callErr != nil {
				entry["error"] = callErr.Error()
				result = "error: " + callErr.Error()
			} else {
				entry["result"] = result
			}
			trace = append(trace, entry)
			messages = append(messages, llm.Message{
				Role: llm.RoleTool, ToolCallID: call.ID, Name: call.Name, Content: result,
			})
		}
	}

	n.emitTrace(input, trace)

	if answer == "" && len(trace) > 0 {
		// The loop exhausted its iterations without a final answer.
		n.event("error", fmt.Sprintf("max iterations %d reached", maxIter))
		return nil, fmt.Errorf("ai-agent: max iterations %d reached without a final answer", maxIter)
	}

	if memory != nil {
		newTurns := []llm.Message{{Role: llm.RoleUser, Content: userPrompt}}
		for i := len(history); i < len(messages); i++ {
			newTurns = append(newTurns, messages[i])
		}
		if err := memory.Append(newTurns...); err != nil {
			n.event("warn", fmt.Sprintf("memory save failed: %v", err))
		}
	}

	n.writeTarget(input, answer)
	n.event("info", "final answer received")
	return input, nil
}

// SelectPorts routes the returned message to output 1 only — the
// trace left through an explicit output-2 emit inside Execute.
func (n *AgentNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	return []string{"output_1"}
}

// emitTrace sends the tool-call trace out of output 2 (best effort —
// a missing edge must not fail the run).
func (n *AgentNode) emitTrace(input nodestore.MessageInterface, trace []interface{}) {
	if n.host == nil {
		return
	}
	msg := nodestore.NewMessage().SetPayload(trace)
	meta := nodebase.CloneMetadata(input)
	if meta == nil {
		meta = map[string]interface{}{}
	}
	msg.SetMetadata(meta)
	if err := n.host.Emit(n.workflowID, n.GetID(), msg, []string{"output_2"}); err != nil {
		n.event("debug", fmt.Sprintf("trace emit failed: %v", err))
	}
}

// options resolves the referenced ai-provider config and applies the
// per-node overrides.
func (n *AgentNode) options() (llm.LlmOptions, error) {
	opts, err := aiprovider.ResolveOptions(n.host, n.workflowID, n.config.Provider)
	if err != nil {
		return llm.LlmOptions{}, err
	}
	if n.config.TimeoutSeconds > 0 {
		opts.Timeout = time.Duration(n.config.TimeoutSeconds * float64(time.Second))
	}
	return opts, nil
}

// callContext derives the per-model-call context honouring the
// configured timeout (default 60s when the node sets none — an agent
// must never block a hop forever). The caller cancels after the call
// so the deadline timer never outlives it.
func (n *AgentNode) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := n.config.TimeoutSeconds
	if timeout <= 0 {
		timeout = 60
	}
	return context.WithTimeout(ctx, time.Duration(timeout*float64(time.Second)))
}

// resolveTools parses the configured references and builds the tool
// declarations the model sees.
func (n *AgentNode) resolveTools() ([]toolTarget, error) {
	if len(nodebase.ParseLinkList(n.config.Tools)) > 0 && n.host == nil {
		return nil, fmt.Errorf("ai-agent: no host — cannot resolve tools")
	}
	var out []toolTarget
	for _, ref := range nodebase.ParseLinkList(n.config.Tools) {
		wf, nodeID, err := aitool.ParseRef(ref, n.workflowID)
		if err != nil {
			return nil, fmt.Errorf("ai-agent: %w", err)
		}
		node, ok := n.host.LookupNode(wf, nodeID)
		if !ok || node == nil {
			return nil, fmt.Errorf("ai-agent: tool %q not found in workflow %q", nodeID, wf)
		}
		decl, err := aitool.ToolForNode(node)
		if err != nil {
			return nil, fmt.Errorf("ai-agent: tool %q: %w", ref, err)
		}
		out = append(out, toolTarget{workflowID: wf, nodeID: nodeID, tool: decl})
	}
	return out, nil
}

// invokeTool executes one model-requested tool call: the arguments go
// in as the payload, the tool flow's ai-tool-return answers through
// the pending-response correlation, and the returned payload becomes
// the tool result fed back to the model.
func (n *AgentNode) invokeTool(ctx context.Context, tools []toolTarget, call llm.ToolCall) (string, error) {
	var target *toolTarget
	for i := range tools {
		if tools[i].tool.Name == call.Name {
			target = &tools[i]
			break
		}
	}
	if target == nil {
		return "", fmt.Errorf("model requested unknown tool %q", call.Name)
	}
	if n.host == nil {
		return "", fmt.Errorf("no host — cannot invoke tool %q", call.Name)
	}

	msg := nodestore.NewMessage().
		SetPayload(aitool.DecodeArguments(call.Arguments)).
		SetMetadata(map[string]interface{}{
			aitool.MetaKey: map[string]interface{}{"id": call.ID, "name": call.Name},
			"aiAgentId":    n.GetID(),
		})

	msgID, ch, err := n.host.InjectMessage(target.workflowID, target.nodeID, msg)
	if err != nil {
		return "", fmt.Errorf("cannot invoke tool %q: %w", call.Name, err)
	}

	timeout := n.config.ToolTimeoutSeconds
	if timeout <= 0 {
		timeout = 60
	}
	select {
	case response := <-ch:
		return payloadString(response.GetPayload()), nil
	case <-time.After(time.Duration(timeout * float64(time.Second))):
		n.host.DiscardPending(msgID)
		return "", fmt.Errorf("tool %q did not return within %v", call.Name, timeout)
	case <-ctx.Done():
		n.host.DiscardPending(msgID)
		return "", ctx.Err()
	}
}

// payloadString renders a tool-return payload as the tool-result
// string fed back to the model — strings pass through, everything
// else is JSON-encoded.
func payloadString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nodebase.StringOf(v)
	}
	return string(raw)
}

// openMemory resolves the optional ai-memory reference and binds the
// conversation for this message. A nil store (no memory configured or
// no context-store seam) yields nil — memory is simply off.
func (n *AgentNode) openMemory(input nodestore.MessageInterface) (*aimemory.Store, error) {
	if n.config.Memory == "" {
		return nil, nil
	}
	if n.host == nil {
		return nil, fmt.Errorf("ai-agent: no host — cannot resolve ai-memory %q", n.config.Memory)
	}
	node, ok := n.host.LookupNode(n.workflowID, n.config.Memory)
	if !ok || node == nil {
		return nil, fmt.Errorf("ai-agent: ai-memory %q not found in workflow", n.config.Memory)
	}
	cfg, err := aimemory.ConfigForNode(node)
	if err != nil {
		return nil, err
	}
	return aimemory.OpenStore(n.host.ResolvedContextStore(), node.GetID(), cfg,
		cfg.ConversationKey(input)), nil
}

// writeTarget stores the final answer — same convention as ai-request.
func (n *AgentNode) writeTarget(input nodestore.MessageInterface, value interface{}) {
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

// acquire takes a slot on the concurrency semaphore (same machinery
// as ai-request).
func (n *AgentNode) acquire(ctx context.Context) (func(), error) {
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

// semaphore returns the in-flight cap channel, or nil when uncapped.
func (n *AgentNode) semaphore() chan struct{} {
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

// event logs a status event through the host. Prompts, keys and tool
// payloads are never logged — only call metadata.
func (n *AgentNode) event(level, message string) {
	if n.host == nil {
		return
	}
	n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), level, message)
}

// Validate checks the structural config: a provider reference is
// required and max-iterations, when set, must be positive.
func (n *AgentNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Provider == "" {
		return fmt.Errorf("ai-agent: provider reference is required")
	}
	if n.config.MaxIterations < 0 {
		return fmt.Errorf("ai-agent: maxIterations must be positive")
	}
	return nil
}
