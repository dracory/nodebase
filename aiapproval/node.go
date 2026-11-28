// Package aiapproval contains the ai-approval node: a
// human-in-the-loop pause. Wired into a flow (typically inside a tool
// flow ahead of ai-tool-return), it emits an approval request —
// question + approvalId — on output 2 and waits for a reply message
// carrying the same approvalId on its input. The pending record lives
// in the host's context store, so a restart re-driving the hop sees
// the same approvalId and either keeps waiting or resumes
// immediately when the decision already landed.
package aiapproval

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/dracory/nodebase"
	"github.com/dracory/nodebase/aitool"
	"github.com/dracory/nodebase/template"
	nodestore "github.com/dracory/nodestore"
)

// Type is the registered node type id.
const Type = "ai-approval"

// ReplyKey is the message-metadata key marking an approval reply:
// a map with "approvalId" (required), "approved" (bool, default true)
// and an optional "comment".
const ReplyKey = "aiApprovalReply"

// DecisionKey is the message-metadata key the resumed message carries:
// {approvalId, approved, comment}.
const DecisionKey = "aiApproval"

// scopePrefix namespaces the pending-approval records in the context
// store.
const scopePrefix = "ai-approval/"

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(Config)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewApprovalNode(n, c, e), nil
}

// init registers the ai-approval node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        Type,
			Name:        "AI Approval",
			Description: "Pauses the flow until a human reply keyed by approvalId arrives; request leaves on output 2",
			Icon:        "check-circle",
			Category:    "AI",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the ai-approval node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"question": map[string]interface{}{
			"type":        "string",
			"title":       "Question",
			"description": "Mustache template rendered against the message — the question the approver answers; empty uses the payload as-is",
		},
		"timeoutSeconds": map[string]interface{}{
			"type":        "number",
			"title":       "Timeout (s)",
			"description": "How long to await the reply; 0 waits until the workflow stops",
		},
	},
}

// Config holds the ai-approval configuration.
type Config struct {
	// Question is the mustache template rendered against the
	// incoming message — the question asked of the approver. Empty
	// stringifies the payload.
	Question string `json:"question"`
	// TimeoutSeconds bounds the wait; <= 0 waits until the run
	// context ends.
	TimeoutSeconds float64 `json:"timeoutSeconds"`
}

// ApprovalNode is the ai-approval executor.
type ApprovalNode struct {
	*nodebase.BaseNode
	config     *Config
	host       nodebase.NodeHost
	workflowID string
}

// NewApprovalNode creates a new ai-approval node.
func NewApprovalNode(node nodestore.NodeInterface, config *Config, host nodebase.NodeHost) *ApprovalNode {
	return &ApprovalNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute routes the message: a reply (metadata aiApprovalReply)
// resolves a pending approval and dies here; anything else is a
// request — the node emits the approval request on output 2, blocks
// for the decision and returns the resumed message on output 1.
func (n *ApprovalNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if reply, ok := input.GetMetadata()[ReplyKey].(map[string]interface{}); ok {
		return input, n.execReply(input, reply)
	}
	return n.execRequest(ctx, input)
}

// SelectPorts kills the reply message after it resolved a pending
// approval; request-resume messages go to output 1.
func (n *ApprovalNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	if output == nil || output.GetMetadata() == nil {
		return nil
	}
	if _, ok := output.GetMetadata()[ReplyKey]; ok {
		return nil
	}
	return []string{"output_1"}
}

// approvalID derives the correlation id for this request. The stamped
// message id is used so a re-driven hop after a restart recomputes
// the same id and finds its stored record.
func (n *ApprovalNode) approvalID(input nodestore.MessageInterface) string {
	if id, _ := input.GetMetadata()[aitool.MsgIDKey].(string); id != "" {
		return "appr-" + id
	}
	if id := input.GetID(); id != "" {
		return "appr-" + id
	}
	return "appr-" + nodebase.GenerateID()
}

// record is the persisted pending-approval state.
type record struct {
	Question string `json:"question"`
	Status   string `json:"status"` // pending | approved | rejected
	Comment  string `json:"comment,omitempty"`
}

// execRequest emits the approval request on output 2 and waits for
// the decision. Restart-safe: the record is persisted before the
// wait, keyed by a deterministic approval id, so a re-driven hop
// re-enters here, re-announces the pending request and resumes the
// moment the stored decision exists.
func (n *ApprovalNode) execRequest(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if n.host == nil {
		return nil, fmt.Errorf("ai-approval: no host — cannot wait for a reply")
	}
	approvalID := n.approvalID(input)

	question, err := n.question(input)
	if err != nil {
		return nil, err
	}

	store := n.host.ResolvedContextStore()
	if store == nil {
		return nil, fmt.Errorf("ai-approval: no context store — cannot persist the pending approval")
	}
	scope := scopePrefix + n.GetID()

	// Re-drive path: a record that already carries a decision resumes
	// without re-emitting or waiting.
	if raw, ok, err := store.GetContext(nodebase.ContextScopeGlobal, scope, approvalID); err != nil {
		return nil, err
	} else if ok {
		if rec := decodeRecord(raw); rec != nil && rec.Status != "" && rec.Status != "pending" {
			return n.resume(input, approvalID, rec)
		}
	} else {
		rec := record{Question: question, Status: "pending"}
		if err := store.SetContext(nodebase.ContextScopeGlobal, scope, approvalID, rec); err != nil {
			return nil, err
		}
	}

	waiters := n.waiters()
	waiters[approvalID] = make(chan map[string]interface{}, 1)
	waiter := waiters[approvalID]
	defer delete(waiters, approvalID)

	// Announce the request on output 2. Fire-and-forget by design —
	// the request fan-out must not block the wait.
	req := nodestore.NewMessage().
		SetPayload(map[string]interface{}{"approvalId": approvalID, "question": question}).
		SetMetadata(map[string]interface{}{"approvalId": approvalID, "question": question})
	if err := n.host.Emit(n.workflowID, n.GetID(), req, []string{"output_2"}); err != nil {
		n.event("warn", fmt.Sprintf("approval request emit failed: %v", err))
	}
	n.event("info", fmt.Sprintf("awaiting approval %s", approvalID))

	var timer <-chan time.Time
	if n.config.TimeoutSeconds > 0 {
		t := time.NewTimer(time.Duration(n.config.TimeoutSeconds * float64(time.Second)))
		defer t.Stop()
		timer = t.C
	}
	runCtx := n.host.RunContext(n.workflowID)
	if runCtx == nil {
		runCtx = ctx
	}

	select {
	case reply := <-waiter:
		return n.resume(input, approvalID, &record{
			Status:  statusOf(reply),
			Comment: stringOf(reply["comment"]),
		})
	case <-timer:
		return nil, fmt.Errorf("ai-approval: timed out awaiting approval %s", approvalID)
	case <-runCtx.Done():
		return nil, runCtx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// execReply resolves a pending approval: the stored record is marked
// decided (idempotent — a duplicate reply is a no-op) and the live
// waiter, when one exists on this runner, is woken.
func (n *ApprovalNode) execReply(input nodestore.MessageInterface, reply map[string]interface{}) error {
	if n.host == nil {
		return fmt.Errorf("ai-approval: no host")
	}
	approvalID, _ := reply["approvalId"].(string)
	if approvalID == "" {
		return fmt.Errorf("ai-approval: reply carries no approvalId")
	}
	store := n.host.ResolvedContextStore()
	scope := scopePrefix + n.GetID()
	if store != nil {
		rec := record{Status: statusOf(reply), Comment: stringOf(reply["comment"])}
		if raw, ok, err := store.GetContext(nodebase.ContextScopeGlobal, scope, approvalID); err == nil && ok {
			if prev := decodeRecord(raw); prev != nil {
				rec.Question = prev.Question
			}
		}
		if err := store.SetContext(nodebase.ContextScopeGlobal, scope, approvalID, rec); err != nil {
			return err
		}
	}
	if waiter, ok := n.waiters()[approvalID]; ok {
		waiter <- reply
	}
	n.event("info", fmt.Sprintf("approval %s replied: %s", approvalID, statusOf(reply)))
	return nil
}

// resume marks the record decided, stamps the decision metadata and
// returns the message for output 1.
func (n *ApprovalNode) resume(input nodestore.MessageInterface, approvalID string, rec *record) (nodestore.MessageInterface, error) {
	if input.GetMetadata() == nil {
		input.SetMetadata(map[string]interface{}{})
	}
	input.GetMetadata()[DecisionKey] = map[string]interface{}{
		"approvalId": approvalID,
		"approved":   rec.Status == "approved",
		"comment":    rec.Comment,
	}
	n.event("info", fmt.Sprintf("approval %s %s — resuming", approvalID, rec.Status))
	return input, nil
}

// waiters returns the per-run approvalId → reply-channel table kept
// in node state (executors are rebuilt per message).
func (n *ApprovalNode) waiters() map[string]chan map[string]interface{} {
	if n.host != nil {
		return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
			return map[string]chan map[string]interface{}{}
		}).(map[string]chan map[string]interface{})
	}
	return map[string]chan map[string]interface{}{}
}

// question renders the configured template, or stringifies the
// payload when no template is set.
func (n *ApprovalNode) question(input nodestore.MessageInterface) (string, error) {
	if n.config.Question == "" {
		if s, ok := input.GetPayload().(string); ok {
			return s, nil
		}
		return nodebase.StringOf(input.GetPayload()), nil
	}
	return template.RenderMustache(n.config.Question, input, n.host, n.workflowID, n.GetID(), false)
}

// statusOf maps a reply's "approved" flag (default true) to the
// stored status word.
func statusOf(reply map[string]interface{}) string {
	if approved, ok := reply["approved"].(bool); ok && !approved {
		return "rejected"
	}
	return "approved"
}

func stringOf(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// decodeRecord normalizes a stored record (JSON-round-tripped map)
// back to a typed record.
func decodeRecord(raw interface{}) *record {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil
	}
	rec := &record{
		Status:  stringOf(m["status"]),
		Comment: stringOf(m["comment"]),
	}
	rec.Question = stringOf(m["question"])
	return rec
}

// event logs a status event through the host.
func (n *ApprovalNode) event(level, message string) {
	if n.host == nil {
		return
	}
	n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), level, message)
}
