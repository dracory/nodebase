// Package join contains the join node implementation.
package join

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(JoinConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	// Node-RED distinguishes an explicitly empty count ("count":"" —
	// "use the sequence's own parts.count") from an unset/numeric one.
	if s, ok := n.GetData()["count"].(string); ok && s == "" {
		c.countFromParts = true
	}
	// Validate reduce expressions at deploy so unsupported syntax
	// fails loudly rather than at message time.
	if c.Mode == "reduce" {
		if err := nodebase.CompileSeqExpr(c.ReduceExp); err != nil {
			return nil, fmt.Errorf("join: invalid reduce expression %q: %w", c.ReduceExp, err)
		}
		if c.ReduceFixup != "" {
			if err := nodebase.CompileSeqExpr(c.ReduceFixup); err != nil {
				return nil, fmt.Errorf("join: invalid fixup expression %q: %w", c.ReduceFixup, err)
			}
		}
	}
	return NewJoinNode(n, c, e), nil
}

// init registers the join node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "join",
			Name:        "Join",
			Description: "Reassembles a message sequence into a single message",
			Icon:        "compress",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the join node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"mode": map[string]interface{}{
			"type":        "string",
			"title":       "Mode",
			"enum":        []string{"auto", "custom", "reduce"},
			"default":     "auto",
			"description": "auto: reassemble a split sequence via parts metadata; custom: join by count/timeout/complete; reduce: fold the sequence with an expression",
		},
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Message property to join; \"payload\" (default) or a message path",
			"default":     "payload",
		},
		"key": map[string]interface{}{
			"type":        "string",
			"title":       "Group Key",
			"description": "Custom mode object build: message property whose value keys the object (default \"topic\")",
		},
		"build": map[string]interface{}{
			"type":    "string",
			"title":   "Build",
			"enum":    []string{"array", "object", "merged", "string"},
			"default": "array",
		},
		"joiner": map[string]interface{}{
			"type":        "string",
			"title":       "Joiner",
			"description": "Delimiter for the \"string\" build (escape sequences like \\n allowed)",
		},
		"count": map[string]interface{}{
			"type":        "integer",
			"title":       "Count",
			"description": "Custom mode: number of messages per group",
		},
		"timeout": map[string]interface{}{
			"type":        "number",
			"title":       "Timeout (seconds)",
			"description": "Custom mode: flush an incomplete group after this delay; 0 = wait forever",
			"default":     0,
		},
	},
}

// JoinConfig holds configuration for the join node — the Node-RED
// join equivalent. Reassembles message sequences (typically produced
// by split) into a single message.
type JoinConfig struct {
	// Mode selects grouping: "auto" uses the message's parts metadata
	// (split→join pairing); "custom" groups by count/timeout/complete;
	// "reduce" folds a parts sequence with ReduceExp
	Mode string `json:"mode"`
	// Property is the message property joined and (for non-auto modes)
	// written on the result; "payload" (default) or a message path
	Property string `json:"property"`
	// PropertyType is Node-RED's selector; "full" joins the whole
	// message object
	PropertyType string `json:"propertyType"`
	// Key is the message property read for the custom "object" build
	// key (default "topic")
	Key string `json:"key"`
	// Build selects the joined shape: "array" | "object" | "merged" | "string"
	Build string `json:"build"`
	// Joiner is the delimiter used by the "string" build; escape
	// sequences like \n are unescaped
	Joiner string `json:"joiner"`
	// JoinerType is Node-RED's joiner selector ("str" unescapes; other
	// values are taken literally)
	JoinerType string `json:"joinerType"`
	// Count is the custom-mode group size (auto mode uses parts.count)
	Count int `json:"count"`
	// Timeout flushes an incomplete custom-mode group after this many
	// seconds (0 = wait forever)
	Timeout float64 `json:"timeout"`
	// Accumulate keeps a custom-mode merged/object group open after
	// each emission so later messages keep merging
	Accumulate bool `json:"accumulate"`
	// UseParts preserves (false strips) parts metadata in custom mode;
	// Node-RED defaults to true
	UseParts *bool `json:"useparts"`
	// ReduceExp folds each sequence message into the accumulator —
	// JSONata-subset with $A (accumulator), $I (parts.index), $N (count)
	ReduceExp string `json:"reduceExp"`
	// ReduceInit is the accumulator's initial value
	ReduceInit string `json:"reduceInit"`
	// ReduceInitType interprets ReduceInit ("num"|"json"|"str"|"bool")
	ReduceInitType string `json:"reduceInitType"`
	// ReduceFixup optionally post-processes the accumulator ($A, $N)
	ReduceFixup string `json:"reduceFixup"`
	// ReduceRight folds the sequence in reverse index order
	ReduceRight bool `json:"reduceRight"`

	// countFromParts is set by the factory when the serialized count is
	// the empty string — Node-RED then takes targetCount/partId from
	// the message's parts metadata.
	countFromParts bool
}

// joinGroup accumulates one group's contributions plus its flush
// timer. value holds the in-progress build: []interface{} for
// array/string/buffer-like payloads, map[string]interface{} for
// object/merged. msg tracks the most recent message (Node-RED
// Object.assign semantics — the latest message's envelope wins).
type joinGroup struct {
	kind         string // "string" | "object" | "merged" | "array" | "buffer" | other
	list         []interface{}
	obj          map[string]interface{}
	currentCount int
	targetCount  int
	joinChar     string
	arrayLen     int
	prop         string // result write-back path
	msg          nodestore.MessageInterface
	timer        *time.Timer
}

// joinState is the per-node buffered state stored on the host
// (executors are rebuilt per message).
type joinState struct {
	mu     sync.Mutex
	groups map[string]*joinGroup
}

// JoinNode reassembles message groups into a single joined message.
// It drops (returns nil) every message until its group completes.
type JoinNode struct {
	*nodebase.BaseNode
	config     *JoinConfig
	host       nodebase.NodeHost
	workflowID string
}

// NewJoinNode creates a new join node
func NewJoinNode(node nodestore.NodeInterface, config *JoinConfig, host nodebase.NodeHost) *JoinNode {
	return &JoinNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Validate rejects unsupported mode/build combinations at deploy time
func (n *JoinNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	switch n.mode() {
	case "auto", "custom", "reduce":
	default:
		return fmt.Errorf("join: unsupported mode %q", n.config.Mode)
	}
	switch n.build() {
	case "array", "object", "merged", "string":
	default:
		return fmt.Errorf("join: unsupported build %q", n.config.Build)
	}
	return nil
}

// Execute buffers the message under its group id. Until the group is
// complete the message is dropped (nil return); on completion a single
// joined message is returned and flows downstream.
func (n *JoinNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if n.mode() == "reduce" {
		return n.executeReduce(input)
	}
	if n.mode() == "auto" {
		return n.executeAuto(input)
	}
	return n.executeCustom(input)
}

// --- auto mode ------------------------------------------------------

// executeAuto reassembles a parts sequence: the build type, join
// character, object key and chunk length all come from msg.parts —
// the node's own build/joiner config is ignored in auto mode.
func (n *JoinNode) executeAuto(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	parts, ok := input.GetMetadata()["parts"].(map[string]interface{})
	if !ok || parts["id"] == nil {
		// Node-RED warns and drops messages that carry no parts.id in
		// auto mode; a msg.reset clears every in-flight group instead.
		if _, hasReset := input.GetMetadata()["reset"]; hasReset {
			state := n.state()
			state.mu.Lock()
			for k, g := range state.groups {
				if g.timer != nil {
					g.timer.Stop()
				}
				delete(state.groups, k)
			}
			state.mu.Unlock()
		}
		return nil, nil
	}
	partID, _ := parts["id"].(string)

	state := n.state()
	state.mu.Lock()
	group := state.groups[partID]
	if group == nil {
		payloadType, _ := parts["type"].(string)
		group = &joinGroup{kind: payloadType, prop: n.property()}
		if p, ok := parts["property"].(string); ok && p != "" {
			group.prop = p
		}
		if ch, ok := parts["ch"]; ok {
			group.joinChar = nodebase.JSString(ch)
		}
		if al, err := nodebase.ToFloat(parts["len"]); err == nil {
			group.arrayLen = int(al)
		}
		if c, err := nodebase.ToFloat(parts["count"]); err == nil {
			group.targetCount = int(c)
		}
		state.groups[partID] = group
	}
	state.mu.Unlock()

	property, _ := nodebase.NodeProperty(input, group.prop)

	state.mu.Lock()
	switch group.kind {
	case "object":
		key, _ := parts["key"].(string)
		if key == "" {
			state.mu.Unlock()
			// Node-RED warns: "missing msg.parts.key — cannot add to object"
			return nil, nil
		}
		if group.obj == nil {
			group.obj = map[string]interface{}{}
		}
		if _, exists := group.obj[key]; !exists {
			group.currentCount++
		}
		group.obj[key] = property
	case "merged":
		if mm, ok := property.(map[string]interface{}); ok {
			if group.obj == nil {
				group.obj = map[string]interface{}{}
			}
			for k, v := range mm {
				if k == "_msgid" {
					continue
				}
				if _, exists := group.obj[k]; !exists {
					group.currentCount++
				}
				group.obj[k] = v
			}
		}
	default:
		// string/array/buffer/unknown types collect by parts.index.
		idx := intIndex(parts)
		for len(group.list) <= idx {
			group.list = append(group.list, nil)
		}
		if group.list[idx] == nil {
			group.currentCount++
		}
		group.list[idx] = property
	}
	group.msg = input
	complete := group.targetCount > 0 && group.currentCount >= group.targetCount
	state.mu.Unlock()

	if complete || hasFlag(input, "complete") {
		return n.completeGroup(partID, true), nil
	}
	return nil, nil
}

// --- custom mode ----------------------------------------------------

// executeCustom groups by a single partId ("_" — or the message's
// parts.id when the serialized count was empty). The build comes from
// config; the group completes when its count of collected values
// reaches targetCount, when msg.complete arrives, or when its timer
// (armed on group creation) fires.
func (n *JoinNode) executeCustom(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	partID := "_"
	targetCount := n.config.Count
	parts, hasParts := input.GetMetadata()["parts"].(map[string]interface{})
	if n.config.countFromParts && hasParts {
		if c, err := nodebase.ToFloat(parts["count"]); err == nil {
			targetCount = int(c)
		}
		if id, ok := parts["id"].(string); ok && id != "" {
			partID = id
		}
	}

	// msg.restartTimeout re-arms a group's timer without contributing
	// a value.
	state := n.state()
	if _, ok := input.GetMetadata()["restartTimeout"]; ok {
		state.mu.Lock()
		if g := state.groups[partID]; g != nil {
			if g.timer != nil {
				g.timer.Stop()
			}
			if n.config.Timeout > 0 {
				g.timer = time.AfterFunc(time.Duration(n.config.Timeout*float64(time.Second)), func() { n.flushTimeout(partID) })
			}
		}
		state.mu.Unlock()
	}

	// msg.reset drops the in-flight group.
	if _, ok := input.GetMetadata()["reset"]; ok {
		state.mu.Lock()
		if g := state.groups[partID]; g != nil {
			if g.timer != nil {
				g.timer.Stop()
			}
			delete(state.groups, partID)
		}
		state.mu.Unlock()
		return nil, nil
	}

	payloadType := n.build()
	property, _ := n.propertyValue(input)

	var propertyKey string
	if payloadType == "object" {
		kv, _ := nodebase.NodeProperty(input, n.key())
		propertyKey = nodebase.JSString(kv)
	}
	if payloadType == "object" && propertyKey == "" {
		// Node-RED warns "missing key property" — unless msg.complete
		// flushes whatever has accumulated.
		if hasFlag(input, "complete") {
			return n.completeGroup(partID, false), nil
		}
		return nil, nil
	}

	state.mu.Lock()
	group := state.groups[partID]
	if group == nil {
		group = &joinGroup{kind: payloadType, targetCount: targetCount, prop: n.property()}
		if payloadType == "object" || payloadType == "merged" {
			group.kind = "object" // value shape shared; build decides emit form
			group.obj = map[string]interface{}{}
		}
		if payloadType == "string" {
			group.joinChar = n.joiner()
		}
		state.groups[partID] = group
		if n.config.Timeout > 0 {
			group.timer = time.AfterFunc(
				time.Duration(n.config.Timeout*float64(time.Second)),
				func() { n.flushTimeout(partID) },
			)
		}
	}
	if group.targetCount <= 0 {
		group.targetCount = targetCount
	}

	switch payloadType {
	case "object":
		if _, exists := group.obj[propertyKey]; !exists {
			group.currentCount++
		}
		group.obj[propertyKey] = property
	case "merged":
		if mm, ok := property.(map[string]interface{}); ok {
			for k, v := range mm {
				if k == "_msgid" {
					continue
				}
				if _, exists := group.obj[k]; !exists {
					group.currentCount++
				}
				group.obj[k] = v
			}
		}
	default:
		// array/string/buffer: append in arrival order.
		if property != nil {
			group.list = append(group.list, property)
			group.currentCount++
		}
	}
	group.msg = input
	tcnt := group.targetCount
	if hasParts {
		if c, err := nodebase.ToFloat(parts["count"]); err == nil && group.targetCount <= 0 {
			tcnt = int(c)
			group.targetCount = tcnt
		}
	}
	complete := tcnt > 0 && group.currentCount >= tcnt
	state.mu.Unlock()

	if complete || hasFlag(input, "complete") {
		return n.completeGroup(partID, false), nil
	}
	return nil, nil
}

// --- reduce mode ----------------------------------------------------

// executeReduce buffers a parts sequence, folds it through the reduce
// expression in parts.index order (reversed for reduceRight), applies
// the optional fixup and returns the result on the last message.
func (n *JoinNode) executeReduce(input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	parts, ok := input.GetMetadata()["parts"].(map[string]interface{})
	if !ok || parts["id"] == nil {
		return input, nil // no sequence — pass through
	}
	partID, _ := parts["id"].(string)
	count := 0
	if c, err := nodebase.ToFloat(parts["count"]); err == nil {
		count = int(c)
	}

	state := n.state()
	state.mu.Lock()
	group := state.groups[partID]
	if group == nil {
		group = &joinGroup{}
		state.groups[partID] = group
	}
	group.currentCount++
	if group.targetCount <= 0 {
		group.targetCount = count
	}
	msgs := append(reduceMsgs(group), input)
	setReduceMsgs(group, msgs)
	done := group.targetCount > 0 && len(msgs) >= group.targetCount
	if done {
		delete(state.groups, partID)
	}
	state.mu.Unlock()

	if !done {
		return nil, nil
	}
	return n.reduceAndSend(msgs, group.targetCount)
}

// reduceMsgs stores the buffered messages on the group list's backing
// field (as interfaces) to reuse joinGroup for reduce groups.
func reduceMsgs(g *joinGroup) []nodestore.MessageInterface {
	out := make([]nodestore.MessageInterface, 0, len(g.list))
	for _, v := range g.list {
		if m, ok := v.(nodestore.MessageInterface); ok {
			out = append(out, m)
		}
	}
	return out
}

func setReduceMsgs(g *joinGroup, msgs []nodestore.MessageInterface) {
	g.list = g.list[:0]
	for _, m := range msgs {
		g.list = append(g.list, m)
	}
}

// reduceAndSend folds msgs through ReduceExp and returns the result
// message (a clone of the last folded message carrying the reduced
// payload, like Node-RED).
func (n *JoinNode) reduceAndSend(msgs []nodestore.MessageInterface, count int) (nodestore.MessageInterface, error) {
	sort.SliceStable(msgs, func(i, j int) bool {
		ii, jj := partsIndexOf(msgs[i]), partsIndexOf(msgs[j])
		if n.config.ReduceRight {
			return ii > jj
		}
		return ii < jj
	})
	accum, err := n.reduceInit()
	if err != nil {
		return nil, err
	}
	var last nodestore.MessageInterface
	for _, m := range msgs {
		last = m
		vars := map[string]interface{}{
			"I": float64(partsIndexOf(m)),
			"N": float64(count),
			"A": accum,
		}
		v, err := nodebase.EvalSeqExpr(n.config.ReduceExp, nodebase.MessageMap(m), vars)
		if err != nil {
			return nil, fmt.Errorf("join: reduce expression failed: %w", err)
		}
		accum = v
	}
	if n.config.ReduceFixup != "" {
		vars := map[string]interface{}{"A": accum, "N": float64(count)}
		v, err := nodebase.EvalSeqExpr(n.config.ReduceFixup, map[string]interface{}{}, vars)
		if err != nil {
			return nil, fmt.Errorf("join: fixup expression failed: %w", err)
		}
		accum = v
	}
	out := nodebase.ClonePayload(last)
	out.SetPayload(accum)
	return out, nil
}

// reduceInit evaluates the configured initial accumulator value —
// Node-RED evaluateNodeProperty: "num"/"json"/"bool" coerce the
// serialized string, everything else stays a string.
func (n *JoinNode) reduceInit() (interface{}, error) {
	switch n.config.ReduceInitType {
	case "num":
		f, err := strconv.ParseFloat(n.config.ReduceInit, 64)
		if err != nil {
			return nil, fmt.Errorf("join: invalid reduceInit %q: %w", n.config.ReduceInit, err)
		}
		return f, nil
	case "json":
		var v interface{}
		if err := json.Unmarshal([]byte(n.config.ReduceInit), &v); err != nil {
			return nil, fmt.Errorf("join: invalid reduceInit JSON %q: %w", n.config.ReduceInit, err)
		}
		return v, nil
	case "bool":
		return n.config.ReduceInit == "true", nil
	case "msg", "flow", "global", "env":
		// Rarely used for reduce; resolve via message-property seams
		// is out of scope — treat as literal.
		return n.config.ReduceInit, nil
	default:
		return n.config.ReduceInit, nil
	}
}

// --- shared ---------------------------------------------------------

// completeGroup drains a group and builds the joined message. When
// keep is false and accumulate is set the group stays open (merged/
// object accumulation semantics); auto mode always closes.
func (n *JoinNode) completeGroup(partID string, auto bool) nodestore.MessageInterface {
	state := n.state()
	state.mu.Lock()
	group := state.groups[partID]
	if group == nil {
		state.mu.Unlock()
		return nil
	}
	if group.timer != nil {
		group.timer.Stop()
		group.timer = nil
	}
	if auto || !n.config.Accumulate {
		delete(state.groups, partID)
	}
	state.mu.Unlock()

	out := nodebase.ClonePayload(group.msg)
	if out.GetMetadata() == nil {
		out.SetMetadata(map[string]interface{}{})
	}
	value := group.joinedValue()
	nodebase.NodeSetProperty(out, group.prop, value)

	// Node-RED pops the parts stack: a nested split's outer parts
	// survive under parts.parts, otherwise parts is removed.
	if parts, ok := out.GetMetadata()["parts"].(map[string]interface{}); ok {
		if outer, ok := parts["parts"]; ok {
			out.GetMetadata()["parts"] = outer
		} else {
			delete(out.GetMetadata(), "parts")
		}
	}
	delete(out.GetMetadata(), "complete")
	return out
}

// joinedValue renders the group's accumulated values into the final
// payload shape: strings are joined with the join character, arrays
// flattened when the source split chunked (arrayLen > 1), objects
// returned as maps.
func (g *joinGroup) joinedValue() interface{} {
	if g.kind == "string" {
		parts := make([]string, 0, len(g.list))
		for _, v := range g.list {
			parts = append(parts, nodebase.JSString(v))
		}
		out := ""
		for i, p := range parts {
			if i > 0 {
				out += g.joinChar
			}
			out += p
		}
		return out
	}
	if g.obj != nil && (g.kind == "object" || g.kind == "merged") {
		return g.obj
	}
	list := g.list
	if g.kind == "array" && g.arrayLen > 1 {
		flat := make([]interface{}, 0, len(list))
		for _, v := range list {
			if arr, ok := v.([]interface{}); ok {
				flat = append(flat, arr...)
			} else {
				flat = append(flat, v)
			}
		}
		list = flat
	}
	if list == nil {
		list = []interface{}{}
	}
	return list
}

// flushTimeout fires when a custom-mode group times out: it builds the
// joined message from whatever arrived and injects it downstream.
func (n *JoinNode) flushTimeout(partID string) {
	out := n.completeGroup(partID, false)
	if out != nil {
		n.emitDownstream(out)
	}
}

// emitDownstream injects a payload into the edges downstream of this
// node, fire-and-forget (same as inject.fire).
func (n *JoinNode) emitDownstream(payload nodestore.MessageInterface) {
	if n.host == nil {
		return
	}
	_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
}

// propertyValue extracts the joined value from a message: the whole
// payload for "payload", the configured path otherwise.
func (n *JoinNode) propertyValue(msg nodestore.MessageInterface) (interface{}, bool) {
	if n.config.PropertyType == "full" {
		return nodebase.MessageMap(msg), true
	}
	return nodebase.NodeProperty(msg, n.property())
}

// state returns (creating if needed) the per-node buffered groups.
func (n *JoinNode) state() *joinState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &joinState{groups: map[string]*joinGroup{}}
	}).(*joinState)
}

// mode returns the configured grouping mode (default "auto")
func (n *JoinNode) mode() string {
	switch n.config.Mode {
	case "custom", "reduce":
		return n.config.Mode
	}
	return "auto"
}

// build returns the configured join shape (default "array")
func (n *JoinNode) build() string {
	switch n.config.Build {
	case "object", "merged", "string":
		return n.config.Build
	}
	return "array"
}

// property returns the configured target property (default "payload")
func (n *JoinNode) property() string {
	if n.config.Property == "" {
		return "payload"
	}
	return n.config.Property
}

// key returns the configured object-build key property (default "topic")
func (n *JoinNode) key() string {
	if n.config.Key == "" {
		return "topic"
	}
	return n.config.Key
}

// joiner returns the string-build delimiter (joinerType "str"
// unescapes \n and friends)
func (n *JoinNode) joiner() string {
	if n.config.JoinerType == "" || n.config.JoinerType == "str" {
		return nodebase.UnescapeDelim(n.config.Joiner)
	}
	return n.config.Joiner
}

// intIndex reads parts.index from a parts map (0 if absent)
func intIndex(parts map[string]interface{}) int {
	if f, err := nodebase.ToFloat(parts["index"]); err == nil {
		return int(f)
	}
	return 0
}

// partsIndexOf reads parts.index from a message's metadata (0 if absent)
func partsIndexOf(msg nodestore.MessageInterface) int {
	parts, ok := msg.GetMetadata()["parts"].(map[string]interface{})
	if !ok {
		return 0
	}
	return intIndex(parts)
}

// hasFlag reports a truthy top-level message flag (msg.complete,
// msg.reset, …) stored in metadata.
func hasFlag(msg nodestore.MessageInterface, name string) bool {
	v, ok := msg.GetMetadata()[name]
	if !ok {
		return false
	}
	if b, isBool := v.(bool); isBool {
		return b
	}
	return v != nil
}
