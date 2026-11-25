// Package delay contains the delay node implementation.
package delay

import (
	"context"
	_ "embed"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(DelayConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewDelayNode(n, c, e), nil
}

// init registers the delay node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "delay",
			Name:        "Delay",
			Description: "Delays each message or limits the rate at which they pass (Node-RED delay)",
			Icon:        "clock",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the delay node.
// Field visibility follows the selected action via showIf clauses —
// Node-RED shows only the options relevant to each mode.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"pauseType": map[string]interface{}{
			"type":    "string",
			"title":   "Action",
			"default": "delay",
			"options": []map[string]interface{}{
				{"value": "delay", "title": "Fixed delay",
					"description": "Hold every message for the configured time"},
				{"value": "delayv", "title": "Variable delay",
					"description": "Hold each message for the duration read from a message property (msg.delay)"},
				{"value": "random", "title": "Random delay",
					"description": "Hold each message a random time inside a range"},
				{"value": "rate", "title": "Rate limit",
					"description": "Limit how many messages pass per period — queued, dropped, or grouped by topic"},
			},
		},
		"timeout": map[string]interface{}{
			"type":        "number",
			"title":       "For",
			"description": "Fixed delay before each message is released",
			"default":     5,
			"minimum":     0,
			"showIf":      map[string]string{"pauseType": "delay"},
		},
		"timeoutUnits": map[string]interface{}{
			"type":    "string",
			"title":   "Units",
			"enum":    []string{"milliseconds", "seconds", "minutes", "hours"},
			"default": "seconds",
			"showIf":  map[string]string{"pauseType": "delay"},
		},
		"delayPath": map[string]interface{}{
			"type":        "string",
			"title":       "Delay Property",
			"description": "Message property holding the delay in milliseconds (e.g. \"delay\" for msg.delay)",
			"default":     "delay",
			"showIf":      map[string]string{"pauseType": "delayv"},
		},
		"randomFirst": map[string]interface{}{
			"type":    "number",
			"title":   "Between",
			"default": 1,
			"minimum": 0,
			"showIf":  map[string]string{"pauseType": "random"},
		},
		"randomLast": map[string]interface{}{
			"type":    "number",
			"title":   "And",
			"default": 5,
			"minimum": 0,
			"showIf":  map[string]string{"pauseType": "random"},
		},
		"randomUnits": map[string]interface{}{
			"type":    "string",
			"title":   "Units",
			"enum":    []string{"milliseconds", "seconds", "minutes", "hours"},
			"default": "seconds",
			"showIf":  map[string]string{"pauseType": "random"},
		},
		"rate": map[string]interface{}{
			"type":        "integer",
			"title":       "Rate",
			"description": "Messages allowed per rate period — they are spread evenly across it",
			"default":     1,
			"minimum":     1,
			"showIf":      map[string]string{"pauseType": "rate"},
		},
		"nbRateUnits": map[string]interface{}{
			"type":    "number",
			"title":   "Per",
			"default": 1,
			"minimum": 0,
			"showIf":  map[string]string{"pauseType": "rate"},
		},
		"rateUnits": map[string]interface{}{
			"type":    "string",
			"title":   "Rate Units",
			"enum":    []string{"second", "minute", "hour"},
			"default": "second",
			"showIf":  map[string]string{"pauseType": "rate"},
		},
		"rateType": map[string]interface{}{
			"type":    "string",
			"title":   "Apply To",
			"default": "all",
			"options": []map[string]interface{}{
				{"value": "all", "title": "All messages",
					"description": "One shared limiter — every message counts against the same rate"},
				{"value": "topic", "title": "Per msg.topic",
					"description": "Independent limiter per topic; only the most recent message per topic is kept"},
			},
			"showIf": map[string]string{"pauseType": "rate"},
		},
		"drop": map[string]interface{}{
			"type":        "boolean",
			"title":       "Drop Intermediate Messages",
			"description": "Keep only the most recent message while rate-limited (off queues them all)",
			"default":     false,
			"showIf":      map[string]string{"pauseType": "rate", "rateType": "all"},
		},
		"allowrate": map[string]interface{}{
			"type":        "boolean",
			"title":       "Allow msg.rate Override",
			"description": "A numeric msg.rate (in ms) replaces the interval until reset",
			"default":     false,
			"showIf":      map[string]string{"pauseType": "rate"},
		},
	},
}

// DelayConfig holds the node's configuration. Numeric fields accept
// strings too — Node-RED serializes them as quoted values in exports,
// so they stay interface{} and are parsed through nodebase.ParseNumeric.
type DelayConfig struct {
	// PauseType selects the action: "delay" (fixed), "delayv" (delay
	// from a message property), "random" (uniform in a range) or "rate"
	// (rate limit). Legacy spellings "timed" and "queue" are aliases.
	PauseType string `json:"pauseType"`
	// Timeout is the fixed delay value; TimeoutUnits one of
	// milliseconds|seconds|minutes|hours.
	Timeout      interface{} `json:"timeout"`
	TimeoutUnits string      `json:"timeoutUnits"`
	// DelayPath names the message property read for the delay in
	// milliseconds in "delayv" mode (default "delay" → msg.delay).
	DelayPath string `json:"delayPath"`
	// RandomFirst/RandomLast bound the random delay range;
	// RandomUnits carries the unit.
	RandomFirst interface{} `json:"randomFirst"`
	RandomLast  interface{} `json:"randomLast"`
	RandomUnits string      `json:"randomUnits"`
	// Rate messages are allowed per NbRateUnits RateUnits; spread evenly.
	Rate        interface{} `json:"rate"`
	NbRateUnits interface{} `json:"nbRateUnits"`
	RateUnits   string      `json:"rateUnits"`
	// RateType scopes the limiter: "all" (one shared) or "topic"
	// (independent per msg.topic — intermediates auto-drop).
	RateType string `json:"rateType"`
	// Drop keeps only the most recent queued message in "all" mode.
	Drop bool `json:"drop"`
	// AllowRate lets a numeric msg.rate (ms) override the interval.
	AllowRate bool `json:"allowrate"`
	// Duration is the legacy config (milliseconds) kept so existing
	// flows keep working; pauseType fields take precedence when set.
	Duration int `json:"duration"`
}

// rateBucket is one limiter lane — "all" mode has a single bucket,
// "topic" mode one per msg.topic. Messages queued (or the latest kept
// in drop mode) are released one per interval via a timer.
type rateBucket struct {
	queue    []nodestore.MessageInterface
	latest   nodestore.MessageInterface
	next     time.Time // earliest next release
	last     time.Time // when the last message was released
	timer    *time.Timer
	interval time.Duration // 0 = use configured interval
}

// heldMsg is one message parked on a release timer — the fixed,
// variable and random delay modes each keep an independent timer per
// message (Node-RED's idList), so a held message can be released early
// by msg.flush or dropped by msg.reset.
type heldMsg struct {
	msg   nodestore.MessageInterface
	timer *time.Timer
	fired bool
}

// delayState lives in the host's per-run state slot — executors are
// rebuilt per message, so the limiter and the held list must not live
// on the node.
type delayState struct {
	mu      sync.Mutex
	buckets map[string]*rateBucket
	held    []*heldMsg // fixed/variable/random messages awaiting release
}

// DelayNode delays or rate-limits messages. Fixed/variable/random
// modes park the message on a timer and return immediately (Node-RED
// parity — sibling wires are not stalled while a message is held);
// rate mode returns the message when the window is free and otherwise
// queues it for a timed host.Emit.
type DelayNode struct {
	*nodebase.BaseNode
	config     *DelayConfig
	host       nodebase.NodeHost
	workflowID string
}

// NewDelayNode creates a new delay node
func NewDelayNode(node nodestore.NodeInterface, config *DelayConfig, host nodebase.NodeHost) *DelayNode {
	return &DelayNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute applies the configured action. A truthy "reset" property
// clears all held messages without sending them; a "flush" property
// releases held messages immediately (all, or that many when numeric).
func (n *DelayNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if input == nil {
		return nil, nil
	}
	meta := input.GetMetadata()
	_, hasReset := meta["reset"]
	flushVal, hasFlush := meta["flush"]

	if n.mode() == "rate" {
		if hasReset {
			n.resetBuckets()
			return nil, nil
		}
		if hasFlush {
			n.flushBuckets(flushVal)
			return nil, nil
		}
		return n.limit(ctx, input), nil
	}

	// Fixed/variable/random hold modes: the release timer is armed and
	// the message parked first — then a reset clears the whole held
	// list (including this message's own timer) and a flush releases
	// the first held messages immediately. A bare flush control
	// message is not itself parked (Node-RED: a msg whose only
	// properties besides the id are "flush" never enters the list).
	n.hold(input, n.holdDuration(input))
	if hasReset {
		n.clearHeld()
		return nil, nil
	}
	if hasFlush {
		n.flushHeld(flushVal)
	}
	return nil, nil
}

// holdDuration resolves how long the message is held for the active
// hold mode (fixed, variable or random).
func (n *DelayNode) holdDuration(input nodestore.MessageInterface) time.Duration {
	switch n.mode() {
	case "delayv":
		return n.variableDelay(input)
	case "random":
		return n.randomDelay()
	default:
		return n.fixedDelay()
	}
}

// hold parks the message on a release timer and registers it in the
// held list — unless it already fired (a zero delay) or it is a bare
// flush control message.
func (n *DelayNode) hold(input nodestore.MessageInterface, d time.Duration) {
	state := n.state()
	entry := &heldMsg{msg: input}
	entry.timer = time.AfterFunc(d, func() { n.releaseHeld(entry) })
	state.mu.Lock()
	defer state.mu.Unlock()
	if entry.fired {
		return // released before registration could happen
	}
	if pureFlushControl(input) {
		entry.timer.Stop()
		return
	}
	state.held = append(state.held, entry)
}

// releaseHeld fires one parked message when its timer expires.
func (n *DelayNode) releaseHeld(entry *heldMsg) {
	state := n.state()
	state.mu.Lock()
	if entry.fired {
		state.mu.Unlock()
		return
	}
	entry.fired = true
	for i, e := range state.held {
		if e == entry {
			state.held = append(state.held[:i], state.held[i+1:]...)
			break
		}
	}
	state.mu.Unlock()
	_ = n.host.Emit(n.workflowID, n.GetID(), entry.msg, nil)
}

// clearHeld drops every parked message without sending it — the
// msg.reset contract for the hold modes.
func (n *DelayNode) clearHeld() {
	state := n.state()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, e := range state.held {
		e.timer.Stop()
		e.fired = true
	}
	state.held = nil
}

// flushHeld releases parked messages immediately — the first n when
// the flush property is numeric, otherwise all of them.
func (n *DelayNode) flushHeld(v interface{}) {
	state := n.state()
	limit := -1
	if f, ok := nodebase.ParseNumeric(v); ok {
		limit = int(f)
	}
	state.mu.Lock()
	var out []nodestore.MessageInterface
	for len(state.held) > 0 && (limit < 0 || len(out) < limit) {
		e := state.held[0]
		state.held = state.held[1:]
		e.timer.Stop()
		e.fired = true
		out = append(out, e.msg)
	}
	state.mu.Unlock()
	for _, msg := range out {
		_ = n.host.Emit(n.workflowID, n.GetID(), msg, nil)
	}
}

// engineMetaKeys are metadata stamped by the runner itself (message
// id, lineage, step counter) — they do not count as message properties
// when deciding whether a flush-carrying message is a bare control
// message.
var engineMetaKeys = map[string]bool{
	"msgId":         true,
	"parentMsgId":   true,
	"stepCount":     true,
	"preserveMsgId": true,
}

// pureFlushControl reports whether the message is a bare flush control
// message — Node-RED's `Object.keys(msg).length === 2 && "flush" in
// msg` rule: no payload and no message properties besides "flush".
// Such a message flushes the held list without entering it; a message
// that also carries a payload or other properties is parked first and
// may be released by its own flush.
func pureFlushControl(msg nodestore.MessageInterface) bool {
	if msg.GetPayload() != nil {
		return false
	}
	meta := msg.GetMetadata()
	if _, ok := meta["flush"]; !ok {
		return false
	}
	for k := range meta {
		if k == "flush" || engineMetaKeys[k] {
			continue
		}
		return false
	}
	return true
}

// mode normalizes pauseType, mapping Node-RED's legacy spellings
// ("timed" → delay, "queue" → rate FIFO) onto the four real modes.
func (n *DelayNode) mode() string {
	switch n.config.PauseType {
	case "delayv", "random", "rate":
		return n.config.PauseType
	case "queue":
		return "rate"
	default:
		return "delay"
	}
}

// fixedDelay resolves the configured fixed delay — timeout/units, with
// the legacy millisecond Duration as fallback (default 1s).
func (n *DelayNode) fixedDelay() time.Duration {
	if v, ok := nodebase.ParseNumeric(n.config.Timeout); ok && v > 0 {
		return time.Duration(v * float64(delayUnit(n.config.TimeoutUnits)))
	}
	if n.config.Duration > 0 {
		return time.Duration(n.config.Duration) * time.Millisecond
	}
	return time.Second
}

// variableDelay resolves the per-message delay (ms) from the property
// named by delayPath — metadata first, then the payload map. A
// numeric value is used verbatim (negative clamps to zero, Node-RED
// parity); absent or unparseable values fall back to the configured
// fixed delay.
func (n *DelayNode) variableDelay(input nodestore.MessageInterface) time.Duration {
	path := n.config.DelayPath
	if path == "" {
		path = "delay"
	}
	clamp := func(f float64) time.Duration {
		if f < 0 {
			f = 0
		}
		return time.Duration(f * float64(time.Millisecond))
	}
	if input != nil {
		if v, ok := nodebase.GetPath(input.GetMetadata(), path); ok {
			if f, ok := nodebase.ParseNumeric(v); ok {
				return clamp(f)
			}
		}
		if m, ok := input.GetPayload().(map[string]interface{}); ok {
			if v, ok := nodebase.GetPath(m, path); ok {
				if f, ok := nodebase.ParseNumeric(v); ok {
					return clamp(f)
				}
			}
		}
	}
	return n.fixedDelay()
}

// randomDelay returns a uniform delay inside [randomFirst, randomLast]
// in randomUnits (default 1..5 seconds, Node-RED's defaults).
func (n *DelayNode) randomDelay() time.Duration {
	lo, _ := nodebase.ParseNumeric(n.config.RandomFirst)
	hi, _ := nodebase.ParseNumeric(n.config.RandomLast)
	if lo <= 0 && hi <= 0 {
		lo, hi = 1, 5
	}
	if hi < lo {
		lo, hi = hi, lo
	}
	unit := float64(delayUnit(n.config.RandomUnits))
	if unit <= 0 {
		unit = float64(time.Second)
	}
	return time.Duration((lo + rand.Float64()*(hi-lo)) * unit)
}

// rateInterval is the configured spacing between releases: the
// rate-period (nbRateUnits × rateUnits) divided across rate messages —
// Node-RED delivers them evenly spread, so 3 msgs in 12 s means one
// every 4 s.
func (n *DelayNode) rateInterval() time.Duration {
	rate, _ := nodebase.ParseNumeric(n.config.Rate)
	if rate <= 0 {
		rate = 1
	}
	nb, _ := nodebase.ParseNumeric(n.config.NbRateUnits)
	if nb <= 0 {
		nb = 1
	}
	unit := delayUnit(n.config.RateUnits)
	if unit <= 0 {
		unit = time.Second
	}
	return time.Duration(nb * float64(unit) / rate)
}

// topicMode reports whether the limiter is per-msg.topic. Topic mode
// always keeps only the most recent message per topic (Node-RED drops
// intermediates automatically there).
func (n *DelayNode) topicMode() bool { return n.config.RateType == "topic" }

// state returns the shared limiter state for this node run.
func (n *DelayNode) state() *delayState {
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &delayState{buckets: map[string]*rateBucket{}}
	}).(*delayState)
}

// bucketKey resolves the limiter lane for a message.
func (n *DelayNode) bucketKey(input nodestore.MessageInterface) string {
	if !n.topicMode() || input == nil || input.GetMetadata() == nil {
		return ""
	}
	if t, ok := input.GetMetadata()["topic"].(string); ok {
		return t
	}
	return ""
}

// limit applies the rate limiter. The message passes through when its
// bucket's window is free; otherwise it is queued for a timed release
// (or kept as the latest in drop mode / topic mode).
func (n *DelayNode) limit(ctx context.Context, input nodestore.MessageInterface) nodestore.MessageInterface {
	state := n.state()
	key := n.bucketKey(input)

	state.mu.Lock()
	b := state.buckets[key]
	if b == nil {
		b = &rateBucket{}
		state.buckets[key] = b
	}

	// msg.rate override (ms) — applies immediately, persists until reset.
	if n.config.AllowRate && input != nil {
		if v, ok := input.GetMetadata()["rate"]; ok {
			if f, ok := nodebase.ParseNumeric(v); ok && f > 0 {
				b.interval = time.Duration(f * float64(time.Millisecond))
				// Node-RED applies the new rate immediately — the
				// pending window is recomputed off the last release.
				b.next = b.last.Add(b.interval)
				if b.timer != nil {
					n.scheduleLocked(b, key, b.interval)
				}
			}
		}
	}
	interval := b.interval
	if interval <= 0 {
		interval = n.rateInterval()
	}

	now := time.Now()
	if !now.Before(b.next) {
		// Window free — emit now; pend the queue's head if any.
		b.next = now.Add(interval)
		b.last = now
		n.scheduleLocked(b, key, interval)
		state.mu.Unlock()
		return input
	}

	// Window busy — the message is held for later.
	keepLatest := n.config.Drop || n.topicMode()
	if keepLatest {
		b.latest = input
	} else if front, _ := input.GetMetadata()["toFront"].(bool); front {
		b.queue = append([]nodestore.MessageInterface{input}, b.queue...)
	} else {
		b.queue = append(b.queue, input)
	}
	n.scheduleLocked(b, key, interval)
	state.mu.Unlock()
	return nil
}

// scheduleLocked (re)arms the bucket's release timer for the pending
// work — must be called with the state mutex held.
func (n *DelayNode) scheduleLocked(b *rateBucket, key string, interval time.Duration) {
	if b.latest == nil && len(b.queue) == 0 {
		return
	}
	wait := time.Until(b.next)
	if wait < 0 {
		wait = 0
	}
	if b.timer != nil {
		b.timer.Stop()
	}
	b.timer = time.AfterFunc(wait, func() { n.release(key, interval) })
}

// release pops one held message from a bucket and emits it downstream,
// rearming the timer when more remain.
func (n *DelayNode) release(key string, interval time.Duration) {
	state := n.state()
	state.mu.Lock()
	b := state.buckets[key]
	if b == nil {
		state.mu.Unlock()
		return
	}
	var msg nodestore.MessageInterface
	if len(b.queue) > 0 {
		msg = b.queue[0]
		b.queue = b.queue[1:]
	} else {
		msg = b.latest
		b.latest = nil
	}
	b.next = time.Now().Add(interval)
	b.last = time.Now()
	more := len(b.queue) > 0 || b.latest != nil
	if more {
		n.scheduleLocked(b, key, interval)
	} else {
		b.timer = nil
	}
	state.mu.Unlock()

	if msg != nil {
		_ = n.host.Emit(n.workflowID, n.GetID(), msg, nil)
	}
}

// resetBuckets clears every held message and rate override — the
// msg.reset contract (all outstanding messages cleared without sending).
func (n *DelayNode) resetBuckets() {
	state := n.state()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, b := range state.buckets {
		if b.timer != nil {
			b.timer.Stop()
		}
	}
	state.buckets = map[string]*rateBucket{}
}

// flushBuckets releases held messages immediately — the first n when
// the flush property is numeric, otherwise all of them.
func (n *DelayNode) flushBuckets(v interface{}) {
	state := n.state()
	state.mu.Lock()
	limit := -1
	if f, ok := nodebase.ParseNumeric(v); ok {
		limit = int(f)
	}
	var out []nodestore.MessageInterface
	for key, b := range state.buckets {
		for len(b.queue) > 0 && (limit < 0 || len(out) < limit) {
			out = append(out, b.queue[0])
			b.queue = b.queue[1:]
		}
		if b.latest != nil && (limit < 0 || len(out) < limit) {
			out = append(out, b.latest)
			b.latest = nil
		}
		if len(b.queue) == 0 && b.latest == nil {
			if b.timer != nil {
				b.timer.Stop()
				b.timer = nil
			}
			delete(state.buckets, key)
		}
	}
	state.mu.Unlock()

	for _, msg := range out {
		_ = n.host.Emit(n.workflowID, n.GetID(), msg, nil)
	}
}

// delayUnit maps a unit name to a duration. Accepts the Node-RED
// spellings (seconds/minute/…) and short aliases (s/min/hr/ms).
func delayUnit(units string) time.Duration {
	switch strings.ToLower(strings.TrimSpace(units)) {
	case "s", "sec", "second", "seconds":
		return time.Second
	case "min", "minute", "minutes":
		return time.Minute
	case "hr", "hour", "hours":
		return time.Hour
	case "", "ms", "millisecond", "milliseconds":
		return time.Millisecond
	default:
		return time.Second
	}
}

