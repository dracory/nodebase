// Package trigger contains the trigger node implementation.
package trigger

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"strings"
	"sync"
	"time"
)

// Follow-up modes — Node-RED's "then" selector. wait sends op2 after
// the delay; loop resends op1 on every interval until reset; block
// drops further messages until a reset (or the delay, when positive)
// releases them.
const (
	modeWait  = "wait"
	modeLoop  = "loop"
	modeBlock = "block"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(TriggerConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewTriggerNode(n, c, e), nil
}

// init registers the trigger node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "trigger",
			Name:        "Trigger",
			Description: "Sends one message immediately, then optionally a second after a delay",
			Icon:        "hourglass-half",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the trigger node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"op1": map[string]interface{}{
			"type":            "string",
			"title":           "First Output",
			"description":     "Value sent immediately on output 1 (parsed per op1type)",
			"format":          "typedinput",
			"typeField":       "op1type",
			"typedInputTypes": []string{"str", "num", "bool", "json", "payload", "date", "bin", "env"},
		},
		"op1type": map[string]interface{}{
			"type":    "string",
			"title":   "First Output Type",
			"enum":    []string{"str", "num", "bool", "json", "payload", "date", "bin", "env"},
			"default": "str",
		},
		"then": map[string]interface{}{
			"type":    "string",
			"title":   "Then",
			"enum":    []string{"wait", "loop", "block"},
			"default": "wait",
			"description": "wait: send the second output after the delay; " +
				"loop: resend the first output every interval until reset; " +
				"block: wait to be reset (imported flows also derive the mode " +
				"from the delay sign — negative = resend, zero = wait to be reset)",
		},
		"op2": map[string]interface{}{
			"type":            "string",
			"title":           "Second Output",
			"description":     "Value sent after the delay (parsed per op2type)",
			"format":          "typedinput",
			"typeField":       "op2type",
			"typedInputTypes": []string{"str", "num", "bool", "json", "payload", "date", "bin", "env"},
			"showIf":          map[string]string{"then": "wait"},
		},
		"op2type": map[string]interface{}{
			"type":    "string",
			"title":   "Second Output Type",
			"enum":    []string{"str", "num", "bool", "json", "payload", "date", "bin", "env"},
			"default": "str",
			"showIf":  map[string]string{"then": "wait"},
		},
		"duration": map[string]interface{}{
			"type":        "number",
			"title":       "Delay",
			"description": "Delay before the second output fires; negative = resend every interval, 0 = wait to be reset",
			"default":     250,
		},
		"units": map[string]interface{}{
			"type":    "string",
			"title":   "Units",
			"enum":    []string{"ms", "s", "min", "hr"},
			"default": "ms",
		},
		"extend": map[string]interface{}{
			"type":        "boolean",
			"title":       "Extend on New Message",
			"description": "Restart the pending timer when a new message arrives",
			"default":     false,
			"showIf":      map[string]string{"then": "wait"},
		},
		"overrideDelay": map[string]interface{}{
			"type":        "boolean",
			"title":       "Override via msg.delay",
			"description": "Let the message's delay metadata replace the configured delay (same units)",
			"default":     false,
		},
		"outputs": map[string]interface{}{
			"type":        "integer",
			"title":       "Outputs",
			"enum":        []int{1, 2},
			"default":     2,
			"description": "2 sends the delayed output on a second port; 1 reuses output 1",
			"showIf":      map[string]string{"then": "wait"},
		},
		"reset": map[string]interface{}{
			"type":        "string",
			"title":       "Reset Payload",
			"description": "A message whose payload equals this value resets the trigger (msg.reset always does)",
		},
		"resetValue": map[string]interface{}{
			"type":        "string",
			"title":       "Reset Value",
			"description": "Alias for reset — a message whose payload equals it resets the trigger",
		},
		"bytopic": map[string]interface{}{
			"type":        "boolean",
			"title":       "Separate Timers per Topic",
			"description": "Keep an independent timer per message topic",
			"default":     false,
		},
	},
}

// TriggerConfig holds configuration for the trigger node — the Node-RED
// trigger equivalent (simplified field names).
type TriggerConfig struct {
	// Op1 / Op1Type configure the value emitted immediately on output_1.
	// Types: str (default) | num | bool | json | payload (pass input
	// through) | date (current timestamp ms) | bin (JSON byte array or
	// base64) | env (environment variable).
	Op1     string `json:"op1"`
	Op1Type string `json:"op1type"`
	// Op2 / Op2Type configure the value emitted when the timer expires.
	Op2     string `json:"op2"`
	Op2Type string `json:"op2type"`
	// Then selects the follow-up mode: "wait" (default, send op2 after
	// the delay), "loop" (resend op1 every interval until reset) or
	// "block" (wait to be reset — further messages are dropped until a
	// reset or the delay expires). Node-RED serializes the mode through
	// the Duration sign instead: negative = loop, zero = block; an
	// empty Then falls back to that convention.
	Then string `json:"then"`
	// Duration is the delay before op2 fires (in Units). Node-RED
	// encodes the then-mode in its sign: negative = resend every
	// |Duration|, zero = wait to be reset.
	Duration float64 `json:"duration"`
	// Units is the duration unit: ms (default) | s | min | hr.
	Units string `json:"units"`
	// Extend restarts the pending timer on each new message.
	Extend bool `json:"extend"`
	// OverrideDelay lets Metadata["delay"] replace the configured
	// duration (same units).
	OverrideDelay bool `json:"overrideDelay"`
	// Outputs selects where op2 goes: 1 = output_1, 2 = output_2.
	Outputs int `json:"outputs"`
	// ByTopic keeps a separate timer per Metadata["topic"].
	ByTopic TopicScope `json:"bytopic"`
	// Reset is Node-RED's reset field: a message whose payload
	// stringifies to this value resets pending timers (in addition to
	// msg.reset).
	Reset string `json:"reset"`
	// ResetValue is an accepted alias for Reset.
	ResetValue string `json:"resetValue"`
}

// resetValue returns the configured payload that resets the trigger —
// the first non-empty of Reset (Node-RED's field) and ResetValue.
func (c *TriggerConfig) resetValue() string {
	if c.Reset != "" {
		return c.Reset
	}
	return c.ResetValue
}

// TopicScope decodes Node-RED's bytopic representations into a bool:
// exported flows carry the enum strings "topic" (per-topic timers) and
// "all" (one shared timer), while the editor schema writes booleans.
// true means per-topic.
type TopicScope bool

// UnmarshalJSON accepts the enum strings, booleans and boolean strings.
func (s *TopicScope) UnmarshalJSON(raw []byte) error {
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case bool:
		*s = TopicScope(x)
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "topic", "true", "1":
			*s = true
		default:
			*s = false
		}
	case float64:
		*s = x != 0
	default:
		*s = false
	}
	return nil
}

// topicState is one topic's pending follow-up: either a timer (the
// delayed op2 in wait mode, the resend interval in loop mode, or the
// block release) or a bare blocked marker in wait-to-be-reset mode
// with no delay — Node-RED's tout === 0 sentinel.
type topicState struct {
	timer   *time.Timer
	blocked bool
}

// triggerState holds the node's pending timers, keyed by topic. It lives
// in the host's per-workflow state map because executors are rebuilt
// per message.
type triggerState struct {
	mu     sync.Mutex
	topics map[string]*topicState
}

// TriggerNode sends one message immediately, then optionally a second
// after a configurable delay — the Node-RED trigger node. The immediate
// output always routes to output_1 (via SelectPorts); the delayed
// output is emitted asynchronously on output_2 (outputs=2) or output_1.
// Metadata["reset"] clears pending timers and drops the message.
type TriggerNode struct {
	*nodebase.BaseNode
	config     *TriggerConfig
	host       nodebase.NodeHost
	workflowID string
}

// NewTriggerNode creates a new trigger node
func NewTriggerNode(node nodestore.NodeInterface, config *TriggerConfig, host nodebase.NodeHost) *TriggerNode {
	return &TriggerNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute emits op1 immediately (returned as the output, routed to
// output_1 by SelectPorts) and arms/rearms the follow-up per the
// then-mode: wait (op2 after the delay), loop (resend op1 every
// interval until reset) or block (drop further messages until a reset
// or the delay expires). A truthy Metadata["reset"] — or a payload
// equal to the configured reset value — clears the topic and drops
// the message.
func (n *TriggerNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	state := n.state()
	topic := n.topic(input)

	if n.isReset(input) {
		n.clearTopic(state, topic)
		return nil, nil // reset forwards nothing
	}

	mode := n.thenMode()
	delay := n.delay(input) // in configured units; sign matters

	switch mode {
	case modeBlock:
		if n.pending(state, topic) {
			return nil, nil // still blocked — the message is dropped
		}
		n.markBlocked(state, topic)
		if delay > 0 {
			// Held with a timeout: the block releases on its own and
			// the second output fires.
			n.armOp2Timer(state, topic, time.Duration(delay*float64(n.unit())), input)
		}
	case modeLoop:
		if delay == 0 {
			// A zero interval degenerates to the blocked sentinel
			// (Node-RED's tout = 0).
			if n.pending(state, topic) {
				return nil, nil
			}
			n.markBlocked(state, topic)
			break
		}
		// Each new message re-emits op1 and restarts the interval.
		out := nodebase.ClonePayload(input)
		out.SetPayload(n.opValue(n.config.Op1Type, n.config.Op1, input))
		n.armLoop(state, topic, absDuration(delay, n.unit()), out)
		return out, nil
	default: // modeWait
		if delay > 0 {
			n.armOp2Timer(state, topic, time.Duration(delay*float64(n.unit())), input)
		}
	}

	out := nodebase.ClonePayload(input)
	out.SetPayload(n.opValue(n.config.Op1Type, n.config.Op1, input))
	return out, nil
}

// SelectPorts routes the immediate output (op1) to output_1 only.
func (n *TriggerNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	return []string{"output_1"}
}

// delay returns the effective delay in the configured units, honoring
// overrideDelay (Metadata["delay"], same units). The sign carries the
// Node-RED then-mode encoding: negative = resend loop, zero = block.
func (n *TriggerNode) delay(input nodestore.MessageInterface) float64 {
	d := n.config.Duration
	if n.config.OverrideDelay && input != nil && input.GetMetadata() != nil {
		if v, ok := input.GetMetadata()["delay"]; ok {
			if f, ok := nodebase.ParseNumeric(v); ok {
				d = f
			}
		}
	}
	return d
}

// absDuration converts a signed delay in the configured units into a
// positive interval (loop mode encodes itself in the sign).
func absDuration(d float64, unit time.Duration) time.Duration {
	if d < 0 {
		d = -d
	}
	return time.Duration(d * float64(unit))
}

// thenMode resolves the follow-up mode. An explicit Then wins;
// otherwise the Node-RED serialization convention applies: a negative
// duration means "resend it every |duration|" and a zero duration
// means "wait to be reset".
func (n *TriggerNode) thenMode() string {
	switch strings.ToLower(strings.TrimSpace(n.config.Then)) {
	case "loop", "resend", "repeat":
		return modeLoop
	case "block", "waitreset", "wait-for-reset":
		return modeBlock
	}
	if n.config.Duration < 0 {
		return modeLoop
	}
	if n.config.Duration == 0 {
		return modeBlock
	}
	return modeWait
}

// isReset reports whether the message resets the trigger: a truthy
// Metadata["reset"], or a payload whose string form equals the
// configured reset value.
func (n *TriggerNode) isReset(input nodestore.MessageInterface) bool {
	if isTruthyMetadata(input, "reset") {
		return true
	}
	rv := n.config.resetValue()
	if rv == "" || input == nil {
		return false
	}
	p := input.GetPayload()
	if p == nil {
		return false
	}
	switch v := p.(type) {
	case string:
		return v == rv
	case []byte:
		return string(v) == rv
	default:
		return fmt.Sprintf("%v", v) == rv
	}
}

// pending reports whether the topic has an armed timer or a block.
func (n *TriggerNode) pending(state *triggerState, topic string) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	ts, ok := state.topics[topic]
	return ok && (ts.timer != nil || ts.blocked)
}

// markBlocked records the wait-to-be-reset sentinel for the topic.
func (n *TriggerNode) markBlocked(state *triggerState, topic string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	ts := state.topics[topic]
	if ts == nil {
		ts = &topicState{}
		state.topics[topic] = ts
	}
	ts.blocked = true
}

// armLoop (re)starts the resend interval for the topic: every tick
// re-emits a clone of the op1 message on output_1 (re-evaluating a
// "date" payload so each resend carries a fresh timestamp).
func (n *TriggerNode) armLoop(state *triggerState, topic string, interval time.Duration, out nodestore.MessageInterface) {
	state.mu.Lock()
	ts := state.topics[topic]
	if ts == nil {
		ts = &topicState{}
		state.topics[topic] = ts
	}
	if ts.timer != nil {
		ts.timer.Stop()
	}
	ts.blocked = false
	metadata := nodebase.CloneMetadata(out)
	ts.timer = time.AfterFunc(interval, func() { n.loopTick(state, ts, topic, interval, metadata, out.GetPayload()) })
	state.mu.Unlock()
}

// loopTick emits one resend and reschedules itself while the topic
// entry is still this loop's.
func (n *TriggerNode) loopTick(state *triggerState, ts *topicState, topic string, interval time.Duration, metadata map[string]interface{}, payload interface{}) {
	state.mu.Lock()
	if state.topics[topic] != ts || ts.timer == nil {
		state.mu.Unlock()
		return
	}
	timer := ts.timer
	state.mu.Unlock()

	if n.config.Op1Type == "date" {
		payload = n.opValue("date", "", nil)
	}
	meta := make(map[string]interface{}, len(metadata))
	for k, v := range metadata {
		meta[k] = v
	}
	msg := nodestore.NewMessage().SetPayload(payload).SetMetadata(meta).SetTimestamp(time.Now())
	_ = n.host.Emit(n.workflowID, n.GetID(), msg, []string{"output_1"})

	state.mu.Lock()
	if state.topics[topic] == ts {
		timer.Reset(interval)
	}
	state.mu.Unlock()
}

// unit returns the duration unit multiplier (default milliseconds).
func (n *TriggerNode) unit() time.Duration {
	switch n.config.Units {
	case "s":
		return time.Second
	case "min":
		return time.Minute
	case "hr":
		return time.Hour
	default:
		return time.Millisecond
	}
}

// armOp2Timer starts (or, with extend, restarts) the timer that emits
// op2 when it expires — the wait-mode delay and the block-mode release.
// Without extend an already-pending timer is left untouched.
func (n *TriggerNode) armOp2Timer(state *triggerState, topic string, delay time.Duration, input nodestore.MessageInterface) {
	state.mu.Lock()
	defer state.mu.Unlock()

	if ts, ok := state.topics[topic]; ok && ts.timer != nil {
		if !n.config.Extend {
			return // pending timer keeps its original deadline
		}
		ts.timer.Reset(delay)
		return
	}

	ts := state.topics[topic]
	if ts == nil {
		ts = &topicState{}
		state.topics[topic] = ts
	}
	metadata := nodebase.CloneMetadata(input)
	// Clone the input so an op2 "payload" type still resolves at fire
	// time (Node-RED's pay: the message that armed the timer).
	armed := nodebase.ClonePayload(input)
	ts.timer = time.AfterFunc(delay, func() {
		state.mu.Lock()
		delete(state.topics, topic)
		state.mu.Unlock()
		n.emitOp2(metadata, armed)
	})
}

// emitOp2 sends the second output: output_2 when the node has two
// outputs, otherwise output_1. Errors are ignored — the workflow may
// have been stopped meanwhile.
func (n *TriggerNode) emitOp2(metadata map[string]interface{}, armed nodestore.MessageInterface) {
	payload := nodestore.NewMessage().SetPayload(n.opValue(n.config.Op2Type, n.config.Op2, armed)).SetMetadata(metadata).SetTimestamp(time.Now())
	port := "output_1"
	if n.config.Outputs == 2 {
		port = "output_2"
	}
	_ = n.host.Emit(n.workflowID, n.GetID(), payload, []string{port})
}

// clearTopic stops the pending timer/block — the message's topic only
// when bytopic is set, otherwise all of them.
func (n *TriggerNode) clearTopic(state *triggerState, topic string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if n.config.ByTopic {
		if ts, ok := state.topics[topic]; ok {
			if ts.timer != nil {
				ts.timer.Stop()
			}
			delete(state.topics, topic)
		}
		return
	}
	for k, ts := range state.topics {
		if ts.timer != nil {
			ts.timer.Stop()
		}
		delete(state.topics, k)
	}
}

// topic returns the timer group key: Metadata["topic"] when bytopic is
// set, otherwise a single shared bucket.
func (n *TriggerNode) topic(input nodestore.MessageInterface) string {
	if !n.config.ByTopic || input == nil || input.GetMetadata() == nil {
		return "default"
	}
	if t, ok := input.GetMetadata()["topic"].(string); ok && t != "" {
		return t
	}
	return "default"
}

// state returns (creating if needed) the per-node timer map.
func (n *TriggerNode) state() *triggerState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &triggerState{topics: map[string]*topicState{}}
	}).(*triggerState)
}

// opValue resolves a configured op value, adding the env type (the
// host's layered node env first, then the process environment — the
// same seam env.get uses) on top of triggerValue.
func (n *TriggerNode) opValue(kind, raw string, input nodestore.MessageInterface) interface{} {
	if kind == "env" {
		rc := &nodebase.RuleContext{Host: n.host, WorkflowID: n.workflowID, NodeID: n.GetID()}
		if v, ok := rc.Env(raw); ok {
			return v
		}
		return os.Getenv(raw)
	}
	return triggerValue(kind, raw, input)
}

// triggerValue parses a configured op value per its type:
// num (numeric coerce), bool, json, payload (pass the input's data
// through — for the delayed op2 the arming message is passed, so
// payload yields its data), date (current timestamp ms), bin (JSON
// byte array or base64), str/empty (raw string).
func triggerValue(kind, raw string, input nodestore.MessageInterface) interface{} {
	switch kind {
	case "date":
		return float64(time.Now().UnixMilli())
	case "bin":
		return decodeBin(raw)
	case "num":
		if f, ok := nodebase.ParseNumeric(raw); ok {
			return f
		}
		return float64(0)
	case "bool":
		return raw == "true"
	case "json":
		var v interface{}
		if err := json.Unmarshal([]byte(raw), &v); err == nil {
			return v
		}
		return nil
	case "payload":
		if input != nil {
			return input.GetPayload()
		}
		return nil
	default:
		return raw
	}
}

// decodeBin parses a "bin" op value into bytes. Node-RED serializes
// the bin typedInput as a JSON array of byte codes; a base64 string
// is accepted too.
func decodeBin(raw string) interface{} {
	raw = strings.TrimSpace(raw)
	var codes []json.Number
	if err := json.Unmarshal([]byte(raw), &codes); err == nil {
		b := make([]byte, len(codes))
		for i, c := range codes {
			if v, err := c.Int64(); err == nil {
				b[i] = byte(v)
			}
		}
		return b
	}
	if b, err := base64.StdEncoding.DecodeString(raw); err == nil {
		return b
	}
	return nil
}

// isTruthyMetadata reports whether Metadata[key] exists and is truthy
// (bool true, or a non-"false"/non-empty string).
func isTruthyMetadata(input nodestore.MessageInterface, key string) bool {
	if input == nil || input.GetMetadata() == nil {
		return false
	}
	v, ok := input.GetMetadata()[key]
	if !ok {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t != "" && t != "false"
	default:
		return true
	}
}

// Validate checks the trigger node configuration
func (n *TriggerNode) Validate() error {
	switch n.config.Op1Type {
	case "", "str", "num", "bool", "json", "payload", "date", "bin", "env":
	default:
		return fmt.Errorf("invalid op1type %q", n.config.Op1Type)
	}
	switch n.config.Op2Type {
	case "", "str", "num", "bool", "json", "payload", "date", "bin", "env":
	default:
		return fmt.Errorf("invalid op2type %q", n.config.Op2Type)
	}
	switch strings.ToLower(strings.TrimSpace(n.config.Then)) {
	case "", "wait", "loop", "resend", "repeat", "block", "waitreset", "wait-for-reset":
	default:
		return fmt.Errorf("invalid then %q", n.config.Then)
	}
	switch n.config.Units {
	case "", "ms", "s", "min", "hr":
	default:
		return fmt.Errorf("invalid units %q", n.config.Units)
	}
	if n.config.Outputs != 0 && n.config.Outputs != 1 && n.config.Outputs != 2 {
		return fmt.Errorf("invalid outputs %d", n.config.Outputs)
	}
	return nil
}
