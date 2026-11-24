// Package sort contains the sort node implementation.
package sort

import (
	"context"
	_ "embed"
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"sort"
	"sync"
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
			"enum":    []string{"asc", "desc"},
			"default": "asc",
		},
		"asNum": map[string]interface{}{
			"type":        "boolean",
			"title":       "Numeric Compare",
			"description": "Coerce compared values to numbers first",
			"default":     false,
		},
		"target": map[string]interface{}{
			"type":        "string",
			"title":       "Target",
			"enum":        []string{"msg", "seq"},
			"default":     "msg",
			"description": "msg: sort the payload array in place; seq: sort a message sequence by seqKey",
		},
		"seqKey": map[string]interface{}{
			"type":        "string",
			"title":       "Sequence Key",
			"description": "seq target: dotted path into each message's payload used as sort key (default \"payload\")",
		},
	},
}

// SortConfig holds configuration for the sort node — the Node-RED
// sort equivalent.
type SortConfig struct {
	// Order is "asc" (default) or "desc"
	Order string `json:"order"`
	// AsNum coerces compared values to numbers first
	AsNum bool `json:"asNum"`
	// Target is "msg" (sort the payload array in place, default) or
	// "seq" (sort a whole message sequence by SeqKey)
	Target string `json:"target"`
	// SeqKey is the dotted path into each message's Data used as the
	// sequence sort key; "payload"/empty compares the Data itself
	SeqKey string `json:"seqKey"`
}

// sortState is the per-node buffered sequence groups stored on the
// host (executors are rebuilt per message).
type sortState struct {
	mu     sync.Mutex
	groups map[string][]nodestore.MessageInterface
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

// executeMsg sorts the payload's array in place and passes it through.
func (n *SortNode) executeMsg(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	arr, ok := input.GetPayload().([]interface{})
	if !ok {
		return input, nil // not an array — pass through unchanged
	}
	n.sortValues(arr)
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
		if parts, ok := msg.GetMetadata()["parts"].(map[string]interface{}); ok {
			parts["index"] = i
			parts["count"] = len(msgs)
		}
	}
	workflow, _ := n.host.RunningFlow(n.workflowID)
	ctx := n.host.RunContext(n.workflowID)
	if workflow == nil {
		return
	}
	for _, msg := range msgs {
		n.host.ExecuteDownstream(ctx, n.workflowID, n.GetID(), workflow, msg, nil)
	}
}

// sortValues orders an array using JS-like compare semantics: numeric
// when both sides coerce (or asNum forces coercion), otherwise lexical
// on the string form.
func (n *SortNode) sortValues(arr []interface{}) {
	sort.SliceStable(arr, func(i, j int) bool {
		c := compareValues(arr[i], arr[j], n.config.AsNum)
		if n.desc() {
			return c > 0
		}
		return c < 0
	})
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

// seqValue extracts the sort key from a sequence message: the whole
// Data for "payload"/empty, else the dotted path inside the data map.
func (n *SortNode) seqValue(msg nodestore.MessageInterface) interface{} {
	key := n.config.SeqKey
	if key == "" || key == "payload" {
		return msg.GetPayload()
	}
	if m, ok := msg.GetPayload().(map[string]interface{}); ok {
		if v, found := nodebase.GetPath(m, key); found {
			return v
		}
	}
	return nil
}

// state returns (creating if needed) the per-node buffered groups.
func (n *SortNode) state() *sortState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &sortState{groups: map[string][]nodestore.MessageInterface{}}
	}).(*sortState)
}

// target returns the configured sort target (default "msg")
func (n *SortNode) target() string {
	if n.config.Target == "seq" {
		return "seq"
	}
	return "msg"
}

// desc reports descending order (default ascending)
func (n *SortNode) desc() bool {
	return n.config.Order == "desc" || n.config.Order == "descending"
}

// compareValues orders two values with JS-like semantics: when asNum
// is set both sides are coerced to numbers; otherwise numbers compare
// numerically, strings lexically, and mixed types by string form.
// Returns -1, 0 or 1.
func compareValues(a, b interface{}, asNum bool) int {
	if asNum {
		if fa, err1 := nodebase.ToFloat(a); err1 == nil {
			if fb, err2 := nodebase.ToFloat(b); err2 == nil {
				return compareFloats(fa, fb)
			}
		}
	}
	_, aStr := a.(string)
	_, bStr := b.(string)
	if !aStr && !bStr {
		if fa, err1 := nodebase.ToFloat(a); err1 == nil {
			if fb, err2 := nodebase.ToFloat(b); err2 == nil {
				return compareFloats(fa, fb)
			}
		}
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	switch {
	case sa < sb:
		return -1
	case sa > sb:
		return 1
	}
	return 0
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
