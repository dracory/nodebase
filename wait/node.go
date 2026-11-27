// Package wait contains the distributed wait node: it durably parks
// messages until a shared notify signal reaches its target count.
package wait

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dracory/nodebase"
	nodetemplate "github.com/dracory/nodebase/template"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, host nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &Config{TargetCount: "1", MergeMode: "wait-wins", PollIntervalMS: 500}
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	if err := validateConfig(c); err != nil {
		return nil, err
	}
	return NewNode(n, c, host), nil
}

func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "wait",
			Name:        "Wait",
			Description: "Durably parks messages until a distributed notify signal reaches its target count",
			Icon:        "hourglass-half",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"signal": map[string]interface{}{
			"type": "string", "title": "Release Signal",
			"description": "Fixed text or a mustache template such as {{payload.id}}; the evaluated value is trimmed and becomes the global distributed signal key; keys are case-sensitive and shared across workflows",
		},
		"targetCount": map[string]interface{}{
			"type": "string", "title": "Target Signal Count", "default": "1",
			"description": "Fixed positive integer or a mustache template that evaluates to one; exactly this many signal increments are consumed on release",
		},
		"timeoutSeconds": map[string]interface{}{
			"type": "number", "title": "Wait Timeout Seconds", "default": 0,
			"description": "0 waits indefinitely; positive values route the parked message to output_2 when the wait expires",
		},
		"mergeMode": map[string]interface{}{
			"type": "string", "title": "Notification Metadata Merge", "enum": []string{"wait-wins", "notify-wins"}, "default": "wait-wins",
			"description": "wait-wins keeps waiting-message metadata on key conflicts; notify-wins lets cached notification metadata overwrite it",
		},
		"pollIntervalMs": map[string]interface{}{
			"type": "number", "title": "Poll Interval Milliseconds", "default": 500,
			"description": "How often the deployed wait node checks the durable coordination store; lower values react faster but cost more queries",
		},
	},
}

// Config holds the wait node configuration.
type Config struct {
	Signal         string      `json:"signal"`
	TargetCount    interface{} `json:"targetCount"`
	TimeoutSeconds interface{} `json:"timeoutSeconds"`
	MergeMode      string      `json:"mergeMode"`
	PollIntervalMS interface{} `json:"pollIntervalMs"`
}

// Node parks and releases distributed waiters.
type Node struct {
	*nodebase.BaseNode
	cfg        *Config
	host       nodebase.NodeHost
	workflowID string
	state      *waitState
}

// NewNode creates a wait node.
func NewNode(node nodestore.NodeInterface, cfg *Config, host nodebase.NodeHost) *Node {
	return &Node{
		BaseNode:   nodebase.NewBaseNode(node),
		cfg:        cfg,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

func validateConfig(c *Config) error {
	if c == nil {
		return fmt.Errorf("wait: missing configuration")
	}
	if strings.TrimSpace(c.Signal) == "" {
		return fmt.Errorf("wait: release signal template is required")
	}
	targetTemplate := targetCountTemplate(c.TargetCount)
	if targetTemplate == "" {
		return fmt.Errorf("wait: target signal count template is required")
	}
	// Fixed target counts are deployment-time configuration and can be
	// validated immediately. Mustache templates depend on the incoming
	// message, so those are still validated after rendering in Execute.
	if !strings.Contains(targetTemplate, "{{") {
		if _, err := parseTargetCount(targetTemplate); err != nil {
			return err
		}
	}
	if c.MergeMode != "wait-wins" && c.MergeMode != "notify-wins" {
		return fmt.Errorf("wait: unsupported merge mode %q", c.MergeMode)
	}
	if _, err := nonNegativeSeconds(c.TimeoutSeconds); err != nil {
		return err
	}
	if _, err := pollInterval(c); err != nil {
		return err
	}
	return nil
}

func nonNegativeSeconds(value interface{}) (float64, error) {
	if value == nil || value == "" {
		return 0, nil
	}
	seconds, ok := nodebase.ParseNumeric(value)
	if !ok || seconds < 0 {
		return 0, fmt.Errorf("wait: timeout must be a non-negative number of seconds")
	}
	return seconds, nil
}

func pollInterval(c *Config) (time.Duration, error) {
	if c.PollIntervalMS == nil || c.PollIntervalMS == "" {
		return 500 * time.Millisecond, nil
	}
	ms, ok := nodebase.ParseNumeric(c.PollIntervalMS)
	if !ok || ms < 10 {
		return 0, fmt.Errorf("wait: poll interval must be at least 10 milliseconds")
	}
	return time.Duration(ms * float64(time.Millisecond)), nil
}

// Validate fails deployment early when the host offers no durable
// coordination store. There is deliberately no in-memory fallback.
func (n *Node) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if err := validateConfig(n.cfg); err != nil {
		return err
	}
	if _, err := nodebase.ResolveCoordinationStore(n.host); err != nil {
		return err
	}
	// Releasing a parked row is only safe when the host can prove the
	// downstream handoff is durable. Ordinary fire-and-forget Emit is not
	// accepted at this boundary.
	_, err := nodebase.ResolveCoordinationDurableEmitter(n.host)
	return err
}

// Execute evaluates the signal and target count, then persists the whole
// message as a waiter. It returns nil immediately: the message is parked in
// the coordination store, not held in this traversal goroutine.
func (n *Node) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if input == nil {
		return nil, fmt.Errorf("wait: input message is required")
	}
	if err := validateConfig(n.cfg); err != nil {
		return nil, err
	}
	store, err := nodebase.ResolveCoordinationStore(n.host)
	if err != nil {
		return nil, err
	}
	key, err := nodetemplate.RenderMustache(n.cfg.Signal, input, n.host, n.workflowID, n.GetID(), false)
	if err != nil {
		return nil, fmt.Errorf("wait: signal template: %w", err)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("wait: evaluated release signal is blank")
	}
	renderedTarget, err := nodetemplate.RenderMustache(targetCountTemplate(n.cfg.TargetCount), input, n.host, n.workflowID, n.GetID(), false)
	if err != nil {
		return nil, fmt.Errorf("wait: target count template: %w", err)
	}
	target, err := parseTargetCount(renderedTarget)
	if err != nil {
		return nil, err
	}
	timeout, err := nonNegativeSeconds(n.cfg.TimeoutSeconds)
	if err != nil {
		return nil, err
	}
	waiter := nodebase.CoordinationWaiter{
		ID:          nodebase.GenerateID(),
		SignalKey:   key,
		WorkflowID:  n.workflowID,
		NodeID:      n.GetID(),
		TargetCount: target,
		Message:     input,
		CreatedAt:   time.Now().UTC(),
	}
	if timeout > 0 {
		waiter.ExpiresAt = waiter.CreatedAt.Add(time.Duration(timeout * float64(time.Second)))
	}
	if err := store.ParkWaiter(ctx, waiter); err != nil {
		return nil, fmt.Errorf("wait: park signal %q: %w", key, err)
	}
	return nil, nil
}

// targetCountTemplate accepts editor strings and decoded JSON numbers while
// keeping one canonical string representation for mustache rendering.
func targetCountTemplate(value interface{}) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func parseTargetCount(rendered string) (int64, error) {
	text := strings.TrimSpace(rendered)
	target, err := strconv.ParseInt(text, 10, 64)
	if err != nil || target <= 0 {
		return 0, fmt.Errorf("wait: target signal count must evaluate to a positive integer, got %q", text)
	}
	return target, nil
}

// Activate starts the node's single lifecycle-managed poller. Parking never
// creates a goroutine per message; this cancellable poller is what makes
// restart-recovered and cross-runner waiters observable to the flow.
func (n *Node) Activate(ctx context.Context) error {
	if err := n.Validate(); err != nil {
		return err
	}
	state := n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &waitState{}
	}).(*waitState)
	n.state = state
	state.start(n.host.RunContext(n.workflowID), n.pollLoop)
	return nil
}

// Stop cancels the poller. Parked rows deliberately remain in the durable
// store so a later deploy/restart can resume them.
func (n *Node) Stop() error {
	if n.state != nil {
		n.state.stop()
	}
	return nil
}

// StopKey dedupes stopper registration for executor shells of one node.
func (n *Node) StopKey() string {
	return "wait:" + n.workflowID + ":" + n.GetID()
}

type waitState struct {
	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
}

func (s *waitState) start(parent context.Context, loop func(context.Context)) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.running = true
	s.mu.Unlock()
	go func() {
		loop(ctx)
		s.mu.Lock()
		s.running = false
		s.cancel = nil
		s.mu.Unlock()
	}()
}

func (s *waitState) stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.running = false
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (n *Node) pollLoop(ctx context.Context) {
	n.pollOnce(ctx)
	interval, err := pollInterval(n.cfg)
	if err != nil {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.pollOnce(ctx)
		}
	}
}

func (n *Node) pollOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	store, err := nodebase.ResolveCoordinationStore(n.host)
	if err != nil {
		n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), "error", err.Error())
		return
	}
	released, expired, pollErr := store.PollWaiters(ctx, n.workflowID, n.GetID())
	for _, waiter := range expired {
		msg := waiter.Message
		if msg == nil {
			n.discardEmptyClaim(ctx, store, waiter)
			continue
		}
		metadata := msg.GetMetadata()
		if metadata == nil {
			metadata = map[string]interface{}{}
		}
		nodebase.SetPath(metadata, "wait.signal", waiter.SignalKey)
		nodebase.SetPath(metadata, "wait.expired", true)
		msg.SetMetadata(metadata)
		n.deliverClaimedWaiter(ctx, store, waiter, msg, "output_2", "expired")
	}
	for _, waiter := range released {
		msg := waiter.Message
		if msg == nil {
			n.discardEmptyClaim(ctx, store, waiter)
			continue
		}
		mergeCachedMetadata(msg, waiter.CachedMetadata, n.cfg.MergeMode)
		metadata := msg.GetMetadata()
		if metadata == nil {
			metadata = map[string]interface{}{}
		}
		nodebase.SetPath(metadata, "wait.signal", waiter.SignalKey)
		msg.SetMetadata(metadata)
		n.deliverClaimedWaiter(ctx, store, waiter, msg, "output_1", "released")
	}
	// PollWaiters may return already-claimed rows together with a later
	// store error. Those rows are owned by this caller and are processed
	// above even when pollErr is non-nil.
	if pollErr != nil && ctx.Err() == nil {
		n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), "error", "wait poll failed: "+pollErr.Error())
	}
}

// deliverClaimedWaiter performs the ownership handoff required by the
// coordination contract: durable emission first, ack only after success,
// nack on failure so the parked row is retried instead of lost.
func (n *Node) deliverClaimedWaiter(ctx context.Context, store nodebase.CoordinationStore, waiter nodebase.CoordinationWaiter, msg nodestore.MessageInterface, port, label string) {
	emit, err := nodebase.ResolveCoordinationDurableEmitter(n.host)
	if err == nil {
		err = emit(n.workflowID, n.GetID(), msg, []string{port})
	}
	if err != nil {
		nackErr := store.NackWaiter(ctx, waiter.ID, waiter.ClaimToken)
		detail := label + " wait durable emission failed: " + err.Error()
		if nackErr != nil {
			detail += "; waiter remains leased until claim expiry because requeue failed: " + nackErr.Error()
		} else {
			detail += "; waiter requeued for retry"
		}
		if ctx.Err() == nil {
			n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), "error", detail)
		}
		return
	}
	if err := store.AckWaiter(ctx, waiter.ID, waiter.ClaimToken); err != nil && ctx.Err() == nil {
		// The downstream handoff succeeded, so do not nack into a fast retry.
		// If the row truly remains, lease expiry may cause a duplicate — the
		// documented at-least-once outcome — but never a silent loss.
		n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), "error", label+" wait acknowledgement failed after durable emission; duplicate retry is possible: "+err.Error())
	}
}

func (n *Node) discardEmptyClaim(ctx context.Context, store nodebase.CoordinationStore, waiter nodebase.CoordinationWaiter) {
	if err := store.AckWaiter(ctx, waiter.ID, waiter.ClaimToken); err != nil && ctx.Err() == nil {
		n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), "error", "empty claimed waiter could not be acknowledged: "+err.Error())
		return
	}
	if ctx.Err() == nil {
		n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), "error", "claimed waiter had no message envelope and was discarded")
	}
}

// mergeCachedMetadata merges dotted notification metadata keys into the
// released message. wait-wins preserves existing waiting-message keys;
// notify-wins overwrites them. Missing metadata maps are created.
func mergeCachedMetadata(msg nodestore.MessageInterface, cached map[string]interface{}, mode string) {
	if len(cached) == 0 {
		return
	}
	metadata := msg.GetMetadata()
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	for key, value := range cached {
		if mode != "notify-wins" {
			if _, exists := nodebase.GetPath(metadata, key); exists {
				continue
			}
		}
		nodebase.SetPath(metadata, key, value)
	}
	msg.SetMetadata(metadata)
}
