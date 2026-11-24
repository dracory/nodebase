// Package batch contains the batch node implementation.
package batch

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"sync"
	"time"
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
			Description: "Groups incoming messages into batches",
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
			"description": "count mode: number of messages per batch",
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
		"honourParts": map[string]interface{}{
			"type":        "boolean",
			"title":       "Honour Parts",
			"description": "count mode: keep separate buffers per parts sequence",
			"default":     false,
		},
	},
}

// BatchConfig holds configuration for the batch node — the Node-RED
// batch equivalent. Groups incoming messages into batches.
type BatchConfig struct {
	// Mode selects grouping: "count" batches every N messages,
	// "interval" batches everything arriving within a time window,
	// "concat" concatenates a parts sequence into one message
	Mode string `json:"mode"`
	// Count is the batch size for "count" mode
	Count int `json:"count"`
	// Overlap keeps the last N messages of a batch for the next batch
	// ("count" mode)
	Overlap int `json:"overlap"`
	// Interval is the window length in seconds for "interval" mode
	Interval float64 `json:"interval"`
	// HonourParts keeps separate buffers per parts.id in count mode
	HonourParts bool `json:"honourParts"`
}

// batchState is the per-node buffered state stored on the host
// (executors are rebuilt per message). bufs holds count-mode buffers
// keyed by parts id ("" when honourParts is off or absent); window
// holds the interval-mode buffer plus its flush timer; parts holds
// concat-mode groups.
type batchState struct {
	mu     sync.Mutex
	bufs   map[string][]nodestore.MessageInterface
	window []nodestore.MessageInterface
	timer  *time.Timer
	parts  map[string][]nodestore.MessageInterface
}

// BatchNode groups incoming messages into batches. It drops (returns
// nil) buffered messages; completed batches go downstream as a single
// message whose Data is an array of the batched payloads.
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
	return nil
}

// Execute buffers and emits a batch message when a batch completes.
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

// executeCount buffers messages and emits a batch every Count
// messages, keeping the last Overlap messages for the next batch.
// With honourParts the buffer is keyed by the message's parts.id.
func (n *BatchNode) executeCount(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	count := n.config.Count
	if count <= 0 {
		count = 1
	}
	key := ""
	if n.config.HonourParts {
		if parts, ok := input.GetMetadata()["parts"].(map[string]interface{}); ok {
			key, _ = parts["id"].(string)
		}
	}

	state := n.state()
	state.mu.Lock()
	buf := append(state.bufs[key], input)
	if len(buf) < count {
		state.bufs[key] = buf
		state.mu.Unlock()
		return nil, nil // keep buffering — drop this message
	}

	batch := buf
	overlap := n.config.Overlap
	if overlap > 0 && overlap < len(buf) {
		rest := make([]nodestore.MessageInterface, overlap)
		copy(rest, buf[len(buf)-overlap:])
		state.bufs[key] = rest
	} else {
		delete(state.bufs, key)
	}
	state.mu.Unlock()

	return batchMessage(batch), nil
}

// executeInterval buffers messages arriving within each Interval
// window; the flush timer emits the batch asynchronously.
func (n *BatchNode) executeInterval(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	interval := n.config.Interval
	if interval <= 0 {
		interval = 1
	}

	state := n.state()
	state.mu.Lock()
	state.window = append(state.window, input)
	if state.timer == nil {
		state.timer = time.AfterFunc(
			time.Duration(interval*float64(time.Second)),
			n.flushInterval,
		)
	}
	state.mu.Unlock()

	return nil, nil // buffered — flushed on window end
}

// flushInterval emits the buffered window as a single batch message.
func (n *BatchNode) flushInterval() {
	state := n.state()
	state.mu.Lock()
	batch := state.window
	state.window = nil
	state.timer = nil
	state.mu.Unlock()

	if len(batch) == 0 {
		return
	}
	n.emitDownstream(batchMessage(batch))
}

// executeConcat buffers a parts sequence until parts.count messages
// arrive, then emits one message whose Data is the concatenated
// payloads (arrays are flattened, other payloads collected).
func (n *BatchNode) executeConcat(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	parts, ok := input.GetMetadata()["parts"].(map[string]interface{})
	if !ok {
		return input, nil // no sequence metadata — pass through
	}
	id, _ := parts["id"].(string)
	count := 0
	if c, err := nodebase.ToFloat(parts["count"]); err == nil {
		count = int(c)
	}
	if count <= 0 {
		return input, nil
	}

	state := n.state()
	state.mu.Lock()
	group := append(state.parts[id], input)
	if len(group) < count {
		state.parts[id] = group
		state.mu.Unlock()
		return nil, nil // keep buffering — drop this message
	}
	delete(state.parts, id)
	state.mu.Unlock()

	return concatMessage(group), nil
}

// emitDownstream injects a payload into the edges downstream of this
// node, fire-and-forget (same as inject.fire).
func (n *BatchNode) emitDownstream(payload nodestore.MessageInterface) {
	_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
}

// state returns (creating if needed) the per-node buffered state.
func (n *BatchNode) state() *batchState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &batchState{
			bufs:  map[string][]nodestore.MessageInterface{},
			parts: map[string][]nodestore.MessageInterface{},
		}
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

// batchMessage builds the batch output: a clone of the last message
// whose Data is an array of each buffered message's Data.
func batchMessage(msgs []nodestore.MessageInterface) nodestore.MessageInterface {
	arr := make([]interface{}, 0, len(msgs))
	for _, m := range msgs {
		arr = append(arr, m.GetPayload())
	}
	out := nodebase.ClonePayload(msgs[len(msgs)-1])
	out.SetPayload(arr)
	return out
}

// concatMessage builds the concat output: a clone of the last message
// whose Data flattens array payloads into one array (or collects
// non-array payloads into an array).
func concatMessage(msgs []nodestore.MessageInterface) nodestore.MessageInterface {
	arr := make([]interface{}, 0, len(msgs))
	for _, m := range msgs {
		if chunk, ok := m.GetPayload().([]interface{}); ok {
			arr = append(arr, chunk...)
		} else {
			arr = append(arr, m.GetPayload())
		}
	}
	out := nodebase.ClonePayload(msgs[len(msgs)-1])
	out.SetPayload(arr)
	return out
}
