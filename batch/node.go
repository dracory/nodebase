// Package batch contains the batch node implementation.
package batch

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(BatchConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewBatchNode(n, c, e), nil
}

// init registers the batch node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "batch",
			Name:        "Batch",
			Description: "Groups incoming messages into batch sequences",
			Icon:        "copy",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the batch node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"mode": map[string]interface{}{
			"type":    "string",
			"title":   "Mode",
			"enum":    []string{"count", "interval", "concat"},
			"default": "count",
		},
		"count": map[string]interface{}{
			"type":        "integer",
			"title":       "Count",
			"description": "count mode: number of messages per batch sequence",
			"default":     10,
		},
		"overlap": map[string]interface{}{
			"type":        "integer",
			"title":       "Overlap",
			"description": "count mode: messages kept for the next batch",
			"default":     0,
		},
		"interval": map[string]interface{}{
			"type":        "number",
			"title":       "Interval (seconds)",
			"description": "interval mode: window length",
			"default":     10,
		},
		"allowEmptySequence": map[string]interface{}{
			"type":        "boolean",
			"title":       "Allow Empty Sequence",
			"description": "interval mode: emit an empty sequence message on a window with no input",
			"default":     false,
		},
		"honourParts": map[string]interface{}{
			"type":        "boolean",
			"title":       "Honour Parts",
			"description": "count mode: flush when an incoming parts sequence completes",
			"default":     false,
		},
		"topics": map[string]interface{}{
			"type":        "array",
			"title":       "Topics",
			"description": "concat mode: topics whose sequences are concatenated, in output order",
			"items": map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{"topic": map[string]interface{}{"type": "string"}},
			},
		},
	},
}

// BatchConfig holds configuration for the batch node — the Node-RED
// batch equivalent. Buffers messages and re-emits them as a parts
// sequence (count/interval) or concatenates topic sequences (concat).
type BatchConfig struct {
	// Mode selects grouping: "count" batches every N messages,
	// "interval" flushes everything buffered every Interval seconds,
	// "concat" merges one complete parts sequence per configured topic
	Mode string `json:"mode"`
	// Count is the batch size for "count" mode
	Count int `json:"count"`
	// Overlap keeps the last N messages of a batch for the next batch
	// ("count" mode)
	Overlap int `json:"overlap"`
	// Interval is the window length in seconds for "interval" mode
	Interval float64 `json:"interval"`
	// AllowEmptySequence emits an empty sequence message when an
	// interval window closes with nothing buffered
	AllowEmptySequence bool `json:"allowEmptySequence"`
	// HonourParts flushes a count-mode batch early when an incoming
	// parts sequence completes (parts.index+1 == parts.count)
	HonourParts bool `json:"honourParts"`
	// Topics lists the concat-mode topics in output order; each entry
	// is {"topic": "..."} as serialized by Node-RED
	Topics []struct {
		Topic string `json:"topic"`
	} `json:"topics"`
}

// batchState is the per-node buffered state stored on the host
// (executors are rebuilt per message).
type batchState struct {
	mu sync.Mutex

	// count mode
	pending []nodestore.MessageInterface
	eof     bool

	// interval mode
	window   []nodestore.MessageInterface
	interval bool // ticker goroutine started

	// concat mode: topic -> gid -> group (gids preserves arrival order)
	topics map[string]*topicPending

	// emission FIFO: sequences drain in order on one worker goroutine
	queue   [][]nodestore.MessageInterface
	emitRun bool
}

// topicPending buffers one topic's sequence groups in gid order.
type topicPending struct {
	gids   []string
	groups map[string]*topicGroup
}

// topicGroup is one parts sequence buffered under a topic.
type topicGroup struct {
	msgs  []nodestore.MessageInterface
	count int // -1 until a parts.count is seen
}

// BatchNode re-emits buffered messages as a parts sequence. Buffered
// messages are dropped (nil return); completed batches go downstream
// asynchronously through the node's outgoing edges.
type BatchNode struct {
	*nodebase.BaseNode
	config     *BatchConfig
	host       nodebase.NodeHost
	workflowID string
}

// NewBatchNode creates a new batch node
func NewBatchNode(node nodestore.NodeInterface, config *BatchConfig, host nodebase.NodeHost) *BatchNode {
	return &BatchNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Validate rejects unsupported modes at deploy time
func (n *BatchNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	switch n.mode() {
	case "count", "interval", "concat":
	default:
		return fmt.Errorf("batch: unsupported mode %q", n.config.Mode)
	}
	if n.mode() == "count" && n.config.Count > 0 && n.config.Overlap >= n.config.Count {
		return fmt.Errorf("batch: overlap (%d) must be less than count (%d)", n.config.Overlap, n.config.Count)
	}
	return nil
}

// Execute buffers the message and, when a batch completes, emits the
// sequence asynchronously — the triggering message itself is dropped.
func (n *BatchNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	switch n.mode() {
	case "interval":
		return n.executeInterval(input)
	case "concat":
		return n.executeConcat(input)
	default:
		return n.executeCount(input)
	}
}

// --- count mode -----------------------------------------------------

// executeCount buffers messages and sends the batch sequence every
// Count messages (or when honourParts sees a sequence end), keeping
// the last Overlap messages for the next batch.
func (n *BatchNode) executeCount(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	count := n.config.Count
	if count <= 0 {
		count = 1
	}

	state := n.state()
	state.mu.Lock()
	if _, ok := input.GetMetadata()["reset"]; ok {
		state.pending = nil
		state.eof = false
		state.mu.Unlock()
		return nil, nil
	}
	if n.config.HonourParts {
		if parts, ok := input.GetMetadata()["parts"].(map[string]interface{}); ok {
			idx, _ := nodebase.ToFloat(parts["index"])
			cnt, _ := nodebase.ToFloat(parts["count"])
			if int(idx)+1 == int(cnt) {
				state.eof = true
			}
		}
	}
	state.pending = append(state.pending, input)
	if len(state.pending) != count && !state.eof {
		state.mu.Unlock()
		return nil, nil // keep buffering
	}
	queue := state.pending
	overlap := n.config.Overlap
	if overlap > 0 && overlap < len(queue) {
		rest := make([]nodestore.MessageInterface, overlap)
		copy(rest, queue[len(queue)-overlap:])
		state.pending = rest
	} else {
		state.pending = nil
	}
	state.eof = false
	state.mu.Unlock()

	n.emitSequence(queue)
	return nil, nil
}

// --- interval mode --------------------------------------------------

// executeInterval buffers messages; a ticker started on the first
// message flushes the window every Interval seconds.
func (n *BatchNode) executeInterval(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	state := n.state()
	state.mu.Lock()
	if _, ok := input.GetMetadata()["reset"]; ok {
		state.window = nil
		state.mu.Unlock()
		return nil, nil
	}
	state.window = append(state.window, input)
	started := state.interval
	state.interval = true
	state.mu.Unlock()

	if !started {
		go n.intervalLoop()
	}
	return nil, nil // buffered — flushed on window end
}

// intervalLoop flushes the buffered window on every tick, mirroring
// Node-RED's setInterval.
func (n *BatchNode) intervalLoop() {
	interval := n.config.Interval
	if interval <= 0 {
		interval = 10 // Node-RED's editor default when unset
	}
	ticker := time.NewTicker(time.Duration(interval * float64(time.Second)))
	defer ticker.Stop()
	for range ticker.C {
		n.flushInterval()
	}
}

// flushInterval emits the buffered window as a parts sequence — or an
// empty sequence when allowEmptySequence is set.
func (n *BatchNode) flushInterval() {
	state := n.state()
	state.mu.Lock()
	window := state.window
	state.window = nil
	state.mu.Unlock()

	if len(window) == 0 {
		if n.config.AllowEmptySequence {
			msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{
				"parts": map[string]interface{}{
					"id":    newID(),
					"index": 0,
					"count": 1,
				},
			})
			n.emitSequence([]nodestore.MessageInterface{msg})
		}
		return
	}
	n.emitSequence(window)
}

// --- concat mode ----------------------------------------------------

// executeConcat buffers parts sequences per (topic, parts.id); when
// every configured topic has a complete sequence the messages are
// concatenated in topic order and re-emitted as one sequence.
func (n *BatchNode) executeConcat(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	topics := n.topicList()
	topic, _ := input.GetMetadata()["topic"].(string)
	if !containsString(topics, topic) {
		return nil, nil // topic not configured — drop
	}
	parts, ok := input.GetMetadata()["parts"].(map[string]interface{})
	if !ok || parts["id"] == nil || parts["index"] == nil || parts["count"] == nil {
		return nil, nil // Node-RED errors "no-parts" — dropped
	}
	gid, _ := parts["id"].(string)
	count, _ := nodebase.ToFloat(parts["count"])

	state := n.state()
	state.mu.Lock()
	if _, ok := input.GetMetadata()["reset"]; ok {
		state.topics = map[string]*topicPending{}
		state.mu.Unlock()
		return nil, nil
	}
	tp := state.topics[topic]
	if tp == nil {
		tp = &topicPending{groups: map[string]*topicGroup{}}
		state.topics[topic] = tp
	}
	g := tp.groups[gid]
	if g == nil {
		g = &topicGroup{count: -1}
		tp.groups[gid] = g
		tp.gids = append(tp.gids, gid)
	}
	g.msgs = append(g.msgs, input)
	if g.count < 0 {
		g.count = int(count)
	}
	combined := tryConcatLocked(state, topics)
	state.mu.Unlock()

	if combined == nil {
		return nil, nil // still waiting on other topics
	}
	n.emitSequence(combined)
	return nil, nil
}

// tryConcatLocked returns the concatenated messages (in topics order)
// when every configured topic's first group is complete — removing the
// groups — or nil while any topic is incomplete. Like Node-RED, the
// messages are collected before anything is removed, so a duplicated
// topic concatenates the same group twice.
func tryConcatLocked(state *batchState, topics []string) []nodestore.MessageInterface {
	for _, topic := range topics {
		tp := state.topics[topic]
		if tp == nil || len(tp.gids) == 0 {
			return nil
		}
		g := tp.groups[tp.gids[0]]
		if g == nil || g.count < 0 || len(g.msgs) < g.count {
			return nil
		}
	}
	var out []nodestore.MessageInterface
	for _, topic := range topics {
		tp := state.topics[topic]
		out = append(out, tp.groups[tp.gids[0]].msgs...)
	}
	for _, topic := range topics {
		tp := state.topics[topic]
		if len(tp.gids) == 0 {
			continue // duplicate topic already removed
		}
		delete(tp.groups, tp.gids[0])
		tp.gids = tp.gids[1:]
	}
	return out
}

// --- shared ---------------------------------------------------------

// emitSequence renumbers each message's parts (id = the first
// message's id, index, count — preserving other parts fields like
// type) and replays them through the downstream edges in order, like
// Node-RED's send_msgs. A single goroutine keeps the sequence ordered.
func (n *BatchNode) emitSequence(msgs []nodestore.MessageInterface) {
	if len(msgs) == 0 {
		return
	}
	emitted := make([]nodestore.MessageInterface, 0, len(msgs))
	// Unique sequence id per emitted batch — the source messages may
	// share a message id (function node.send clones), and a reused id
	// would merge distinct batches inside a downstream join.
	msgID := fmt.Sprintf("batch-%s-%d", n.GetID(), nodebase.NextGroupSeq())
	for i, msg := range msgs {
		em := nodestore.NewMessage().SetID(msg.GetID()).SetPayload(msg.GetPayload()).SetTimestamp(msg.GetTimestamp())
		if err := msg.GetError(); err != nil {
			em.SetError(*err)
		}
		em.SetMetadata(deepCopyMetadata(msg.GetMetadata()))
		if em.GetMetadata() == nil {
			em.SetMetadata(map[string]interface{}{})
		}
		parts, _ := em.GetMetadata()["parts"].(map[string]interface{})
		if parts == nil {
			parts = map[string]interface{}{}
			em.GetMetadata()["parts"] = parts
		}
		parts["id"] = msgID
		parts["index"] = i
		parts["count"] = len(msgs)
		emitted = append(emitted, em)
	}
	// Serialize emissions on the node's FIFO worker — overlapping
	// goroutines would interleave sequences at a downstream join.
	state := n.state()
	state.mu.Lock()
	state.queue = append(state.queue, emitted)
	if !state.emitRun {
		state.emitRun = true
		state.mu.Unlock()
		go n.drainEmits()
		return
	}
	state.mu.Unlock()
}

// drainEmits walks the emission queue FIFO until empty; each sequence
// propagates fully (synchronous traverse per message) before the next.
func (n *BatchNode) drainEmits() {
	workflow, _ := n.host.RunningFlow(n.workflowID)
	ctx := n.host.RunContext(n.workflowID)
	state := n.state()
	for {
		state.mu.Lock()
		if len(state.queue) == 0 {
			state.emitRun = false
			state.mu.Unlock()
			return
		}
		seq := state.queue[0]
		state.queue = state.queue[1:]
		state.mu.Unlock()
		if workflow == nil {
			continue
		}
		for _, msg := range seq {
			n.host.ExecuteDownstream(ctx, n.workflowID, n.GetID(), workflow, msg, nil)
		}
	}
}

// deepCopyMetadata copies a metadata map via JSON so nested maps
// (parts) detach from the original — shallow copies leave them shared
// and racy.
func deepCopyMetadata(md map[string]interface{}) map[string]interface{} {
	if md == nil {
		return nil
	}
	raw, err := json.Marshal(md)
	if err != nil {
		return md
	}
	var out map[string]interface{}
	if json.Unmarshal(raw, &out) != nil {
		return md
	}
	return out
}

// topicList returns the configured concat topics in order.
func (n *BatchNode) topicList() []string {
	out := make([]string, 0, len(n.config.Topics))
	for _, t := range n.config.Topics {
		out = append(out, t.Topic)
	}
	return out
}

// state returns (creating if needed) the per-node buffered state.
func (n *BatchNode) state() *batchState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &batchState{topics: map[string]*topicPending{}}
	}).(*batchState)
}

// mode returns the configured batch mode (default "count")
func (n *BatchNode) mode() string {
	switch n.config.Mode {
	case "interval", "concat":
		return n.config.Mode
	}
	return "count"
}

// newID returns a sequence id for synthetic messages.
func newID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

// containsString reports whether list contains s.
func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
