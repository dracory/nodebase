// Package trigger contains the trigger node implementation.
package trigger

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"sync"
	"time"
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
			"type":        "string",
			"title":       "First Output",
			"description": "Value sent immediately on output 1 (parsed per op1type)",
		},
		"op1type": map[string]interface{}{
			"type":    "string",
			"title":   "First Output Type",
			"enum":    []string{"str", "num", "bool", "json", "payload"},
			"default": "str",
		},
		"op2": map[string]interface{}{
			"type":        "string",
			"title":       "Second Output",
			"description": "Value sent after the delay (parsed per op2type)",
		},
		"op2type": map[string]interface{}{
			"type":    "string",
			"title":   "Second Output Type",
			"enum":    []string{"str", "num", "bool", "json", "payload"},
			"default": "str",
		},
		"duration": map[string]interface{}{
			"type":        "number",
			"title":       "Delay",
			"description": "Delay before the second output fires (0 = none)",
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
	// Types: str (default) | num | bool | json | payload (pass input through).
	Op1     string `json:"op1"`
	Op1Type string `json:"op1type"`
	// Op2 / Op2Type configure the value emitted when the timer expires.
	Op2     string `json:"op2"`
	Op2Type string `json:"op2type"`
	// Duration is the delay before op2 fires (0 = no second message).
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
	ByTopic bool `json:"bytopic"`
}

// triggerState holds the node's pending timers, keyed by topic. It lives
// in the host's per-workflow state map because executors are rebuilt
// per message.
type triggerState struct {
	mu     sync.Mutex
	timers map[string]*time.Timer
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
// output_1 by SelectPorts) and arms/rearms the op2 timer. A truthy
// Metadata["reset"] clears pending timers and drops the message.
func (n *TriggerNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	state := n.state()
	topic := n.topic(input)

	if isTruthyMetadata(input, "reset") {
		n.clearTimers(state, topic)
		return nil, nil // reset forwards nothing
	}

	delay := n.delay(input)
	if delay > 0 {
		n.armTimer(state, topic, delay, input)
	}

	out := nodebase.ClonePayload(input)
	out.SetPayload(triggerValue(n.config.Op1Type, n.config.Op1, input))
	return out, nil
}

// SelectPorts routes the immediate output (op1) to output_1 only.
func (n *TriggerNode) SelectPorts(input, output nodestore.MessageInterface) []string {
	return []string{"output_1"}
}

// delay returns the effective delay before op2 fires, honoring
// overrideDelay (Metadata["delay"], same units as configured).
func (n *TriggerNode) delay(input nodestore.MessageInterface) time.Duration {
	d := n.config.Duration
	if n.config.OverrideDelay && input != nil && input.GetMetadata() != nil {
		if v, ok := input.GetMetadata()["delay"]; ok {
			if f, ok := nodebase.ParseNumeric(v); ok {
				d = f
			}
		}
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(d * float64(n.unit()))
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

// armTimer starts (or, with extend, restarts) the op2 timer for the
// topic. Without extend an already-pending timer is left untouched.
func (n *TriggerNode) armTimer(state *triggerState, topic string, delay time.Duration, input nodestore.MessageInterface) {
	state.mu.Lock()
	defer state.mu.Unlock()

	if t, ok := state.timers[topic]; ok {
		if !n.config.Extend {
			return // pending timer keeps its original deadline
		}
		t.Reset(delay)
		return
	}

	metadata := nodebase.CloneMetadata(input)
	state.timers[topic] = time.AfterFunc(delay, func() {
		state.mu.Lock()
		delete(state.timers, topic)
		state.mu.Unlock()

		payload := nodestore.NewMessage().SetPayload(triggerValue(n.config.Op2Type, n.config.Op2, nil)).SetMetadata(metadata).SetTimestamp(time.Now())
		// Port-scoped emission: op2 goes out output_2 when the node has
		// two outputs, otherwise shares output_1. Errors are ignored —
		// the workflow may have been stopped meanwhile.
		port := "output_1"
		if n.config.Outputs == 2 {
			port = "output_2"
		}
		_ = n.host.Emit(n.workflowID, n.GetID(), payload, []string{port})
	})
}

// clearTimers stops pending timers — the message's topic only when
// bytopic is set, otherwise all of them.
func (n *TriggerNode) clearTimers(state *triggerState, topic string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if n.config.ByTopic {
		if t, ok := state.timers[topic]; ok {
			t.Stop()
			delete(state.timers, topic)
		}
		return
	}
	for k, t := range state.timers {
		t.Stop()
		delete(state.timers, k)
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
		return &triggerState{timers: map[string]*time.Timer{}}
	}).(*triggerState)
}

// triggerValue parses a configured op value per its type:
// num (numeric coerce), bool, json, payload (pass the input's data
// through — for the delayed op2 there is no input, so payload yields
// nil), str/empty (raw string).
func triggerValue(kind, raw string, input nodestore.MessageInterface) interface{} {
	switch kind {
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
	case "", "str", "num", "bool", "json", "payload":
	default:
		return fmt.Errorf("invalid op1type %q", n.config.Op1Type)
	}
	switch n.config.Op2Type {
	case "", "str", "num", "bool", "json", "payload":
	default:
		return fmt.Errorf("invalid op2type %q", n.config.Op2Type)
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
