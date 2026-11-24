// Package join contains the join node implementation.
package join

import (
	"context"
	_ "embed"
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"sort"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(JoinConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
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
			"enum":        []string{"auto", "custom"},
			"default":     "auto",
			"description": "auto: reassemble a split sequence via parts metadata; custom: group by a key property",
		},
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Field to join; \"payload\" (default) or a dotted path inside the payload",
			"default":     "payload",
		},
		"key": map[string]interface{}{
			"type":        "string",
			"title":       "Group Key",
			"description": "Custom mode: dotted path into the payload whose value groups messages (default \"topic\")",
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
	// (split→join pairing); "custom" groups by the Key property
	Mode string `json:"mode"`
	// Property is the message field joined; "payload" (default) targets
	// msg.GetPayload(), otherwise a dotted path inside the data map
	Property string `json:"property"`
	// Key is the dotted path into Data whose value groups messages in
	// custom mode (e.g. "topic")
	Key string `json:"key"`
	// Build selects the joined shape: "array" | "object" | "merged" | "string"
	Build string `json:"build"`
	// Joiner is the delimiter used by the "string" build; escape
	// sequences like \n are unescaped
	Joiner string `json:"joiner"`
	// Count is the custom-mode group size (auto mode uses parts.count)
	Count int `json:"count"`
	// Timeout flushes an incomplete custom-mode group after this many
	// seconds (0 = wait forever)
	Timeout float64 `json:"timeout"`
}

// joinGroup buffers one group's messages plus its flush timer.
type joinGroup struct {
	msgs  []nodestore.MessageInterface
	timer *time.Timer
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
	case "auto", "custom":
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

// Execute buffers the message under its group key. Until the group is
// complete the message is dropped (nil return); on completion a single
// joined message is returned and flows downstream.
func (n *JoinNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	state := n.state()

	groupKey, expected, auto := n.groupOf(input)
	if auto && expected <= 0 {
		// No parts metadata — nothing to reassemble; pass through.
		return input, nil
	}
	if !auto && expected <= 0 {
		return nil, fmt.Errorf("join: custom mode requires a positive count")
	}

	state.mu.Lock()
	group := state.groups[groupKey]
	if group == nil {
		group = &joinGroup{}
		state.groups[groupKey] = group
		// Custom mode: arm the flush timer when the first message of a
		// group arrives and a timeout is configured.
		if !auto && n.config.Timeout > 0 {
			group.timer = time.AfterFunc(
				time.Duration(n.config.Timeout*float64(time.Second)),
				func() { n.flushTimeout(groupKey) },
			)
		}
	}
	group.msgs = append(group.msgs, input)

	if len(group.msgs) < expected {
		state.mu.Unlock()
		return nil, nil // keep buffering — drop this message
	}

	msgs := group.msgs
	if group.timer != nil {
		group.timer.Stop()
	}
	delete(state.groups, groupKey)
	state.mu.Unlock()

	return n.buildMessage(msgs, auto), nil
}

// flushTimeout fires when a custom-mode group times out: it builds the
// joined message from whatever arrived and injects it downstream.
func (n *JoinNode) flushTimeout(groupKey string) {
	state := n.state()
	state.mu.Lock()
	group := state.groups[groupKey]
	if group == nil || len(group.msgs) == 0 {
		state.mu.Unlock()
		return
	}
	msgs := group.msgs
	delete(state.groups, groupKey)
	state.mu.Unlock()

	n.emitDownstream(n.buildMessage(msgs, false))
}

// emitDownstream injects a payload into the edges downstream of this
// node, fire-and-forget (same as inject.fire).
func (n *JoinNode) emitDownstream(payload nodestore.MessageInterface) {
	_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
}

// groupOf resolves the group key and expected message count. In auto
// mode both come from the message's parts metadata; in custom mode the
// key is read from the configured path and the count from config.
// The third return value reports auto mode.
func (n *JoinNode) groupOf(input nodestore.MessageInterface) (string, int, bool) {
	if n.mode() == "auto" {
		parts, ok := input.GetMetadata()["parts"].(map[string]interface{})
		if !ok {
			return "", 0, true // caller passes through
		}
		id, _ := parts["id"].(string)
		count := 0
		if c, err := nodebase.ToFloat(parts["count"]); err == nil {
			count = int(c)
		}
		return "auto:" + id, count, true
	}

	var keyVal interface{}
	key := n.config.Key
	if key == "" {
		key = "topic"
	}
	if m, ok := input.GetPayload().(map[string]interface{}); ok {
		keyVal, _ = nodebase.GetPath(m, key)
	}
	return fmt.Sprintf("custom:%v", keyVal), n.config.Count, false
}

// buildMessage assembles the group's messages into a single joined
// payload. In auto mode messages are ordered by parts.index first.
// The result is a clone of the last message with the joined value
// written at the configured property.
func (n *JoinNode) buildMessage(msgs []nodestore.MessageInterface, auto bool) nodestore.MessageInterface {
	if auto {
		sort.SliceStable(msgs, func(i, j int) bool {
			return partsIndex(msgs[i]) < partsIndex(msgs[j])
		})
	}

	var joined interface{}
	switch n.build() {
	case "array":
		arr := make([]interface{}, 0, len(msgs))
		for _, m := range msgs {
			arr = append(arr, n.propertyValue(m))
		}
		joined = arr
	case "object":
		obj := make(map[string]interface{}, len(msgs))
		for i, m := range msgs {
			key := fmt.Sprintf("%d", i)
			if parts, ok := m.GetMetadata()["parts"].(map[string]interface{}); ok {
				if k, ok := parts["key"].(string); ok {
					key = k
				}
			}
			obj[key] = n.propertyValue(m)
		}
		joined = obj
	case "merged":
		merged := map[string]interface{}{}
		for _, m := range msgs {
			if mm, ok := n.propertyValue(m).(map[string]interface{}); ok {
				for k, v := range mm {
					merged[k] = v
				}
			}
		}
		joined = merged
	case "string":
		delim := nodebase.UnescapeDelim(n.config.Joiner)
		s := ""
		for i, m := range msgs {
			if i > 0 {
				s += delim
			}
			s += fmt.Sprintf("%v", n.propertyValue(m))
		}
		joined = s
	}

	out := nodebase.ClonePayload(msgs[len(msgs)-1])
	if n.property() == "payload" {
		out.SetPayload(joined)
	} else {
		m, ok := out.GetPayload().(map[string]interface{})
		if !ok {
			m = map[string]interface{}{}
			out.SetPayload(m)
		}
		nodebase.SetPath(m, n.property(), joined)
	}
	return out
}

// propertyValue extracts the joined field from a message: the whole
// Data for "payload", else the dotted path inside the data map.
func (n *JoinNode) propertyValue(msg nodestore.MessageInterface) interface{} {
	if n.property() == "payload" {
		return msg.GetPayload()
	}
	if m, ok := msg.GetPayload().(map[string]interface{}); ok {
		if v, found := nodebase.GetPath(m, n.property()); found {
			return v
		}
	}
	return nil
}

// state returns (creating if needed) the per-node buffered groups.
func (n *JoinNode) state() *joinState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &joinState{groups: map[string]*joinGroup{}}
	}).(*joinState)
}

// mode returns the configured grouping mode (default "auto")
func (n *JoinNode) mode() string {
	if n.config.Mode == "custom" {
		return "custom"
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

// partsIndex reads parts.index from a message's metadata (0 if absent)
func partsIndex(msg nodestore.MessageInterface) int {
	parts, ok := msg.GetMetadata()["parts"].(map[string]interface{})
	if !ok {
		return 0
	}
	if f, err := nodebase.ToFloat(parts["index"]); err == nil {
		return int(f)
	}
	return 0
}
