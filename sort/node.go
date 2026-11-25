// Package sort contains the sort node implementation.
package sort

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(SortConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewSortNode(n, c, e), nil
}

// init registers the sort node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "sort",
			Name:        "Sort",
			Description: "Sorts a payload array or a message sequence",
			Icon:        "sort",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the sort node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"order": map[string]interface{}{
			"type":    "string",
			"title":   "Order",
			"enum":    []string{"ascending", "descending"},
			"default": "ascending",
		},
		"as_num": map[string]interface{}{
			"type":        "boolean",
			"title":       "Numeric Compare",
			"description": "Coerce compared values to numbers first",
			"default":     false,
		},
		"targetType": map[string]interface{}{
			"type":        "string",
			"title":       "Target",
			"enum":        []string{"msg", "seq"},
			"default":     "msg",
			"description": "msg: sort the array at target in place; seq: sort a message sequence by seqKey",
		},
		"target": map[string]interface{}{
			"type":        "string",
			"title":       "Target Property",
			"description": "msg target: message property holding the array to sort (default \"payload\")",
		},
		"msgKey": map[string]interface{}{
			"type":        "string",
			"title":       "Element Key",
			"description": "msg target: per-element key — evaluated as a sequence expression when msgKeyType is \"jsonata\"",
		},
		"seqKey": map[string]interface{}{
			"type":        "string",
			"title":       "Sequence Key",
			"description": "seq target: message path or sequence expression used as sort key (default \"payload\")",
		},
	},
}

// SortConfig holds configuration for the sort node — the Node-RED
// sort equivalent (order/as_num/targetType/msgKey/seqKey fields).
type SortConfig struct {
	// Order is "ascending" (default) or "descending" — the "asc"/"desc"
	// spellings from older serialization are also accepted
	Order string `json:"order"`
	// AsNum coerces compared values to numbers first (serialized "as_num")
	AsNum bool `json:"as_num"`
	// Target is the msg-target property path holding the array to
	// sort (default "payload"); ignored for seq target
	Target string `json:"target"`
	// TargetType selects the sort target: "msg" sorts the array at
	// Target in place, "seq" sorts a parts sequence by SeqKey
	TargetType string `json:"targetType"`
	// MsgKey is the msg-target element key: msgKeyType "jsonata"
	// evaluates it against each element, anything else compares the
	// elements themselves
	MsgKey string `json:"msgKey"`
	// MsgKeyType is "elem" (compare elements) or "jsonata"
	MsgKeyType string `json:"msgKeyType"`
	// SeqKey is the seq-target sort key: seqKeyType "msg" reads a
	// message property path, "jsonata" evaluates it against the message
	SeqKey string `json:"seqKey"`
	// SeqKeyType is "msg" or "jsonata"
	SeqKeyType string `json:"seqKeyType"`
}

// sortState is the per-node buffered sequence groups stored on the
// host (executors are rebuilt per message).
type sortState struct {
	mu     sync.Mutex
	groups map[string][]nodestore.MessageInterface
	// emission FIFO: sequences drain in order on one worker goroutine
	queue   [][]nodestore.MessageInterface
	emitRun bool
}

// SortNode sorts either a payload array in place ("msg" target) or a
// buffered parts sequence re-emitted in order ("seq" target).
type SortNode struct {
	*nodebase.BaseNode
	config     *SortConfig
	host       nodebase.NodeHost
	workflowID string
}

// NewSortNode creates a new sort node
func NewSortNode(node nodestore.NodeInterface, config *SortConfig, host nodebase.NodeHost) *SortNode {
	return &SortNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Validate rejects unsupported target/order combinations
func (n *SortNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	switch n.target() {
	case "msg", "seq":
	default:
		return fmt.Errorf("sort: unsupported target %q", n.config.Target)
	}
	return nil
}

// Execute sorts a payload array in place (msg target) or buffers a
// parts sequence until complete (seq target), re-emitting the sorted
// messages asynchronously in order and dropping the current one.
func (n *SortNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if n.target() == "seq" {
		return n.executeSeq(input)
	}
	return n.executeMsg(input)
}

// executeMsg sorts the array at the configured target property in
// place and passes it through.
func (n *SortNode) executeMsg(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	target := n.config.Target
	if target == "" {
		target = "payload"
	}
	v, _ := nodebase.NodeProperty(input, target)
	arr, ok := v.([]interface{})
	if !ok {
		return input, nil // not an array — pass through unchanged
	}
	n.sortValues(arr)
	nodebase.NodeSetProperty(input, target, arr)
	return input, nil
}

// executeSeq buffers messages by parts.id until parts.count arrive,
// then emits them downstream in sorted order via async injection.
func (n *SortNode) executeSeq(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
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
	group := append(state.groups[id], input)
	if len(group) < count {
		state.groups[id] = group
		state.mu.Unlock()
		return nil, nil // keep buffering — drop this message
	}
	delete(state.groups, id)
	state.mu.Unlock()

	n.sortMessages(group)

	// Re-emit the sorted messages downstream in order (async — a node
	// can't return multiple payloads from Execute). A single goroutine
	// runs the synchronous BFS per message so ordering is preserved.
	go n.emitSequence(group)
	return nil, nil
}

// emitSequence replays each buffered message through the downstream
// edges in order. ExecuteDownstream is synchronous, so iterating in
// one goroutine preserves the sorted sequence. Upstream re-numbers
// parts.index to the sorted position before sending.
func (n *SortNode) emitSequence(msgs []nodestore.MessageInterface) {
	for i, msg := range msgs {
		// Detach the emitted copy's metadata before renumbering —
		// the buffered original may still be journaled by the engine
		// on another goroutine, and writing parts on it would race.
		em := nodestore.NewMessage().SetID(msg.GetID()).SetPayload(msg.GetPayload()).SetTimestamp(msg.GetTimestamp())
		if err := msg.GetError(); err != nil {
			em.SetError(*err)
		}
		em.SetMetadata(deepCopyMetadata(msg.GetMetadata()))
		msgs[i] = em
		if parts, ok := em.GetMetadata()["parts"].(map[string]interface{}); ok {
			parts["index"] = i
			parts["count"] = len(msgs)
		}
	}
	n.queueEmission(msgs)
}

// queueEmission serializes sequence emissions on the node's FIFO
// worker — overlapping goroutines would interleave sequences at a
// downstream join/batch.
func (n *SortNode) queueEmission(msgs []nodestore.MessageInterface) {
	state := n.state()
	state.mu.Lock()
	state.queue = append(state.queue, msgs)
	if !state.emitRun {
		state.emitRun = true
		state.mu.Unlock()
		go n.drainEmits()
		return
	}
	state.mu.Unlock()
}

// drainEmits walks the emission queue FIFO until empty.
func (n *SortNode) drainEmits() {
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
// (parts, link call stacks) detach from the original — shallow copies
// leave them shared and racy.
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

// sortValues orders an array using JavaScript compare semantics
// (JSCompare); msgKeyType "jsonata" maps each element through the
// msgKey expression before comparing.
func (n *SortNode) sortValues(arr []interface{}) {
	keyFn := n.msgElemKey()
	sort.SliceStable(arr, func(i, j int) bool {
		c := compareValues(keyFn(arr[i]), keyFn(arr[j]), n.config.AsNum)
		if n.desc() {
			return c > 0
		}
		return c < 0
	})
}

// msgElemKey returns the element key function for msg-target sorts:
// the identity for msgKeyType "elem"/empty, or the msgKey expression
// evaluated with the element as context.
func (n *SortNode) msgElemKey() func(interface{}) interface{} {
	if n.config.MsgKeyType != "jsonata" || n.config.MsgKey == "" {
		return func(v interface{}) interface{} { return v }
	}
	expr := n.config.MsgKey
	return func(v interface{}) interface{} {
		out, err := nodebase.EvalSeqExpr(expr, v, nil)
		if err != nil {
			return v
		}
		return out
	}
}

// sortMessages orders buffered sequence messages by the seqKey value.
func (n *SortNode) sortMessages(msgs []nodestore.MessageInterface) {
	sort.SliceStable(msgs, func(i, j int) bool {
		c := compareValues(n.seqValue(msgs[i]), n.seqValue(msgs[j]), n.config.AsNum)
		if n.desc() {
			return c > 0
		}
		return c < 0
	})
}

// seqValue extracts the sort key from a sequence message: seqKeyType
// "jsonata" evaluates the seqKey expression against the whole message
// (msg.payload.price style paths); "msg"/empty reads seqKey as a
// message property path (default "payload").
func (n *SortNode) seqValue(msg nodestore.MessageInterface) interface{} {
	if n.config.SeqKeyType == "jsonata" {
		v, err := nodebase.EvalSeqExpr(n.config.SeqKey, nodebase.MessageMap(msg), nil)
		if err != nil {
			return nil
		}
		return v
	}
	key := n.config.SeqKey
	if key == "" {
		key = "payload"
	}
	v, _ := nodebase.NodeProperty(msg, key)
	return v
}

// state returns (creating if needed) the per-node buffered groups.
func (n *SortNode) state() *sortState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &sortState{groups: map[string][]nodestore.MessageInterface{}}
	}).(*sortState)
}

// target returns the configured sort target (default "msg")
func (n *SortNode) target() string {
	if n.config.TargetType == "seq" || n.config.Target == "seq" {
		return "seq"
	}
	return "msg"
}

// desc reports descending order (default ascending)
func (n *SortNode) desc() bool {
	return n.config.Order == "desc" || n.config.Order == "descending"
}

// compareValues orders two values with JavaScript semantics: as_num
// coerces both sides through Number() first (NaN compares last);
// otherwise JSCompare applies > between strings lexically and numbers
// numerically. Returns -1, 0 or 1.
func compareValues(a, b interface{}, asNum bool) int {
	if asNum {
		fa, okA := nodebase.JSNumber(a)
		fb, okB := nodebase.JSNumber(b)
		if !okA || !okB {
			return -1 // NaN: "a > b" and "a === b" both false
		}
		return compareFloats(fa, fb)
	}
	return nodebase.JSCompare(a, b)
}

// compareFloats returns -1/0/1 ordering two floats
func compareFloats(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
