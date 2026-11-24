// Package rbe contains the rbe node implementation.
package rbe

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(RBEConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewRBENode(n, c, e), nil
}

// init registers the rbe node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "rbe",
			Name:        "RBE",
			Description: "Only forwards the message when the watched property changed",
			Icon:        "filter",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the rbe node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"func": map[string]interface{}{
			"type":    "string",
			"title":   "Mode",
			"enum":    []string{"rbe", "rbei", "narrowband", "narrowbandEq", "deadband", "deadbandEq"},
			"default": "rbe",
			"description": "rbe: block until the value changes; rbei: pass the first message; " +
				"narrowband/deadband: compare against a gap around the last passed value",
		},
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Field to watch; \"payload\" (default) or a dotted path inside the payload",
			"default":     "payload",
		},
		"gap": map[string]interface{}{
			"type":        "string",
			"title":       "Gap",
			"description": "deadband/narrowband threshold; a trailing % makes it a percentage of the last value",
			"default":     "0",
		},
		"septopics": map[string]interface{}{
			"type":        "boolean",
			"title":       "Separate Topics",
			"description": "Keep a separate previous value per message topic",
			"default":     false,
		},
		"initialValue": map[string]interface{}{
			"type":        "string",
			"title":       "Initial Value",
			"description": "Seed the previous value before any message arrives (JSON or plain string)",
		},
	},
}

// RBEConfig holds configuration for the rbe node — the Node-RED
// report-by-exception equivalent.
type RBEConfig struct {
	// Func is the comparison mode:
	//   rbe          — pass when the value changes (first message passes)
	//   rbei         — like rbe but the first (initial) message is blocked
	//   narrowband   — pass while at-or-within gap of the last passed value
	//   narrowbandEq — pass while strictly within gap
	//   deadband     — pass once the value moved by more than gap
	//   deadbandEq   — pass once the value moved by gap or more
	Func string `json:"func"`
	// Property is the dotted path into the payload; "payload" (the
	// default) targets the whole payload
	Property string `json:"property"`
	// Gap is the deadband/narrowband threshold; a trailing "%" turns it
	// into a percentage of the last passed value
	Gap string `json:"gap"`
	// SepTopics keeps a separate previous value per message topic
	SepTopics bool `json:"septopics"`
	// Start / InitialValue seed the previous value before any message
	Start        string `json:"start"`
	InitialValue string `json:"initialValue"`
}

// rbeState holds the per-node map of previously passed values, stored
// on the host (executors are rebuilt per message).
type rbeState struct {
	mu   sync.Mutex
	prev map[string]interface{}
}

// RBENode forwards a message only when the watched property changed
// (or moved beyond the configured deadband) relative to the last value
// it passed — the Node-RED rbe equivalent.
type RBENode struct {
	*nodebase.BaseNode
	config     *RBEConfig
	host       nodebase.NodeHost
	workflowID string
}

// NewRBENode creates a new rbe node
func NewRBENode(node nodestore.NodeInterface, config *RBEConfig, host nodebase.NodeHost) *RBENode {
	return &RBENode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute compares the watched property against the last passed value
// for the message's topic. Non-passing messages are dropped (nil, nil).
// A "reset" key in Metadata clears the stored state first.
func (n *RBENode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	state := n.state()
	topic := n.topic(input)

	if input.GetMetadata() != nil {
		if _, reset := input.GetMetadata()["reset"]; reset {
			state.mu.Lock()
			if n.config.SepTopics && topic != "" {
				delete(state.prev, topic)
			} else {
				state.prev = map[string]interface{}{}
			}
			state.mu.Unlock()
		}
	}

	value, found := nodebase.RangeProperty(input, n.property())
	if !found {
		// Upstream drops messages that lack the watched property
		// entirely — no send, no state update.
		return nil, nil
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	prev, seen := state.prev[topic]

	passes, err := n.passes(value, prev, seen)
	if err != nil {
		return nil, err
	}
	if passes {
		state.prev[topic] = value
		return input, nil
	}
	// Band modes compare against the last PASSED value; rbe/rbei track
	// the last SEEN value so a blocked first message still seeds prev.
	if n.config.Func == "rbe" || n.config.Func == "rbei" || n.config.Func == "" {
		state.prev[topic] = value
	}
	return nil, nil
}

// passes reports whether the message should be forwarded. prev is only
// meaningful when seen is true; a passed value replaces prev. Band modes
// error on non-numeric input (Node-RED reports a node error, catchable
// by a catch node).
func (n *RBENode) passes(value, prev interface{}, seen bool) (bool, error) {
	switch n.config.Func {
	case "rbe":
		// Upstream rbe sends on any change, including the first value.
		return !seen || !reflect.DeepEqual(value, prev), nil
	case "rbei":
		// Upstream rbei ("ignore initial value") blocks the first
		// message, then sends on change.
		return seen && !reflect.DeepEqual(value, prev), nil
	case "narrowband", "narrowbandEq", "deadband", "deadbandEq":
		// Numeric validation happens before the first-message pass —
		// upstream warns (and sends nothing) on non-numeric input.
		if _, ok := nodebase.ParseNumeric(value); !ok {
			return false, fmt.Errorf("rbe: non-numeric value %v in %s mode", value, n.config.Func)
		}
		if !seen {
			return true, nil
		}
		return n.passBand(value, prev)
	}
	// Unknown/empty func defaults to rbe semantics — safe pass-through
	// on change, which is the least surprising behaviour.
	return !seen || !reflect.DeepEqual(value, prev), nil
}

// passBand evaluates deadband/narrowband comparisons. Both sides must
// coerce numerically; non-numeric values are a node error. The "Eq"
// variants include the boundary, matching upstream: narrowband passes
// at-or-within the gap, narrowbandEq strictly within; deadband passes
// strictly beyond the gap, deadbandEq at-or-beyond.
func (n *RBENode) passBand(value, prev interface{}) (bool, error) {
	v, ok1 := nodebase.ParseNumeric(value)
	p, ok2 := nodebase.ParseNumeric(prev)
	if !ok1 || !ok2 {
		return false, fmt.Errorf("rbe: non-numeric value %v in %s mode", value, n.config.Func)
	}
	diff := math.Abs(v - p)
	gap := n.gap(p)
	switch n.config.Func {
	case "narrowband":
		return diff <= gap, nil
	case "narrowbandEq":
		return diff < gap, nil
	case "deadband":
		return diff > gap, nil
	case "deadbandEq":
		return diff >= gap, nil
	}
	return false, nil
}

// gap returns the comparison threshold; a "10%" gap is a percentage of
// the (absolute) previous value.
func (n *RBENode) gap(prev float64) float64 {
	g := strings.TrimSpace(n.config.Gap)
	if strings.HasSuffix(g, "%") {
		pct, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(g, "%")), 64)
		if err == nil {
			return math.Abs(prev) * pct / 100
		}
		return 0
	}
	f, err := strconv.ParseFloat(g, 64)
	if err != nil {
		return 0
	}
	return math.Abs(f)
}

// topic returns the grouping key for septopics mode: Metadata["topic"]
// first, then a "topic" field inside the payload, else "".
func (n *RBENode) topic(input nodestore.MessageInterface) string {
	if !n.config.SepTopics {
		return ""
	}
	if input.GetMetadata() != nil {
		if t, ok := input.GetMetadata()["topic"].(string); ok {
			return t
		}
	}
	if m, ok := input.GetPayload().(map[string]interface{}); ok {
		if t, ok := m["topic"].(string); ok {
			return t
		}
	}
	return ""
}

// property returns the configured property path (default "payload")
func (n *RBENode) property() string {
	if n.config.Property == "" {
		return "payload"
	}
	return n.config.Property
}

// state returns (creating if needed) the per-node previous-value map,
// seeded with the configured start/initialValue.
func (n *RBENode) state() *rbeState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		s := &rbeState{prev: map[string]interface{}{}}
		seed := n.config.InitialValue
		if seed == "" {
			seed = n.config.Start
		}
		if seed != "" {
			s.prev[""] = nodebase.ParseJSONOrString(seed)
		}
		return s
	}).(*rbeState)
}
