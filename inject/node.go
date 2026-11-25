// Package inject contains the inject node implementation.
package inject

import (
	"context"
	_ "embed"
	"encoding/json"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(InjectConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewInjectNode(n, c, e), nil
}

// init registers the inject node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "inject",
			Name:        "Inject",
			Description: "Manually or periodically injects a message",
			Icon:        "play-circle",
			Category:    "Triggers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the inject node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"payloadType": map[string]interface{}{
			"type":        "string",
			"title":       "Payload Type",
			"enum":        []string{"timestamp", "date", "json", "string", "num", "bool", "env"},
			"description": "timestamp/date: current time in ms · json: parse the payload field · string: literal text · num: parsed number · bool: true/false · env: payload names an environment variable",
			"default":     "timestamp",
		},
		"payload": map[string]interface{}{
			"type":        "string",
			"format":      "textarea",
			"title":       "Payload",
			"description": "Data injected as the message — interpreted per Payload Type (unused for timestamp/date)",
		},
		"topic": map[string]interface{}{
			"type":        "string",
			"title":       "Topic",
			"description": "Optional topic stamped into the message metadata (Node-RED msg.topic)",
		},
		"once": map[string]interface{}{
			"type":        "boolean",
			"title":       "Fire Once on Deploy",
			"description": "Inject the payload immediately when the workflow is deployed",
			"default":     false,
		},
		"onceDelay": map[string]interface{}{
			"type":        "number",
			"title":       "After a Delay of",
			"description": "Seconds to wait before the initial inject (Node-RED's onceDelay, default 0.1)",
			"default":     0.1,
			"minimum":     0,
			"showIf":      map[string]string{"once": "true"},
		},
		"repeatType": map[string]interface{}{
			"type":    "string",
			"title":   "Repeat",
			"default": "none",
			"options": []map[string]interface{}{
				{"value": "none", "title": "None",
					"description": "No automatic repeat — fire manually or once on deploy"},
				{"value": "interval", "title": "Interval",
					"description": "Repeat every fixed period"},
				{"value": "times", "title": "At Specific Times",
					"description": "Fire at up to three times of day, optionally on selected weekdays"},
			},
		},
		"interval": map[string]interface{}{
			"type":        "number",
			"title":       "Interval",
			"description": "Repeat period in Interval Units; 0 = no repeat",
			"default":     0,
			"showIf":      map[string]string{"repeatType": "interval"},
		},
		"intervalUnits": map[string]interface{}{
			"type":        "string",
			"title":       "Interval Units",
			"enum":        []string{"s", "min", "hr", "day"},
			"default":     "s",
			"description": "Time unit for the repeat interval",
			"showIf":      map[string]string{"repeatType": "interval"},
		},
		"times": map[string]interface{}{
			"type":        "array",
			"title":       "Times",
			"description": "Times of day to fire (Node-RED's \"at a specific time\") — up to three entries",
			"items": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"time": map[string]interface{}{
						"type":        "string",
						"title":       "Time",
						"description": "Time of day, 24h local time — \"HH:MM\" (optional \":SS\")",
					},
					"days": map[string]interface{}{
						"type":        "array",
						"title":       "Weekdays",
						"description": "Optional weekday filter — 0=Sunday to 6=Saturday; empty fires every day",
						"items": map[string]interface{}{
							"type": "integer",
							"enum": []int{0, 1, 2, 3, 4, 5, 6},
						},
					},
				},
			},
			"showIf": map[string]string{"repeatType": "times"},
		},
		"props": map[string]interface{}{
			"type":        "array",
			"title":       "Properties",
			"description": "Node-RED-style message properties to inject — each {p, v, vt}. \"payload\" sets msg.Payload, \"topic\" the topic, other names land in message metadata.",
			"items": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"p": map[string]string{
						"type":  "string",
						"title": "Property",
					},
					"v": map[string]interface{}{
						"type":            "string",
						"title":           "Value",
						"format":          "typedinput",
						"typeField":       "vt",
						"typedInputTypes": []string{"str", "num", "bool", "json", "env", "msg", "date"},
					},
				},
			},
		},
	},
}

// InjectConfig holds configuration for the inject node
type InjectConfig struct {
	// Payload is the message data — interpreted per PayloadType
	Payload string `json:"payload"`
	// PayloadType selects how Payload is interpreted: "timestamp"
	// (current time in ms, Node-RED's default), "date" (same),
	// "json" (parsed), "string"/"str" (literal text), "num" (parsed
	// number), "bool" ("true"/"false"), or "env" (Payload names the
	// environment variable to read). Empty defaults to timestamp when
	// no payload is configured, json otherwise.
	PayloadType string `json:"payloadType"`
	// Topic is stamped into Metadata["topic"] on every injected message
	// (Node-RED msg.topic)
	Topic string `json:"topic"`
	// Once fires the payload immediately on deploy
	Once bool `json:"once"`
	// OnceDelay delays the Once fire by this many seconds — the
	// Node-RED field name, serialized as a number in exports (strings
	// are accepted too). 0 or absent fires immediately.
	OnceDelay interface{} `json:"onceDelay"`
	// RepeatType is the editor's repeat-mode selector:
	// "none" | "interval" | "times". It is a UI hint only — at run
	// time the populated fields decide (see repeatMode), so imported
	// configs that carry Node-RED's repeat/crontab fields still
	// schedule correctly.
	RepeatType string `json:"repeatType"`
	// Repeat is Node-RED's repeat field — the fixed interval in
	// seconds (serialized as a string in exports). Used when Interval
	// is unset.
	Repeat interface{} `json:"repeat"`
	// Crontab is Node-RED's cron field — one or more "M H * * DOW"
	// expressions (a leading seconds field is also accepted), split by
	// newline or ";". Parsed into time-of-day entries on deploy when
	// Times is empty.
	Crontab string `json:"crontab"`
	// Times is the "at a specific time" schedule — up to three
	// time-of-day entries, each with an optional weekday filter.
	Times []InjectTime `json:"times"`
	// Interval is the repeat period (0 = no repeat), in IntervalUnits
	Interval float64 `json:"interval"`
	// IntervalUnits scales Interval: "s" (default) | "min" | "hr" | "day"
	IntervalUnits string `json:"intervalUnits"`
	// Props is Node-RED's modern property list — each entry sets one
	// message property: "payload" and "topic" are the well-known ones;
	// anything else lands in Metadata. When a "payload" prop exists it
	// takes precedence over the legacy Payload/PayloadType fields.
	Props []InjectProp `json:"props"`
}

// InjectTime is one "at a specific time" schedule entry: a wall-clock
// time of day plus an optional weekday filter.
type InjectTime struct {
	// Time is "HH:MM" (24h local time); an optional ":SS" seconds
	// field is accepted for finer-grained schedules.
	Time string `json:"time"`
	// Days filters the firing weekdays (0=Sunday … 6=Saturday).
	// Empty fires every day.
	Days []int `json:"days"`
}

// InjectProp is one entry of Node-RED's inject props array:
// {"p":"payload","v":"Hello","vt":"str"}.
type InjectProp struct {
	P  string `json:"p"`  // property name ("payload", "topic", or custom)
	V  string `json:"v"`  // literal value (per VT)
	VT string `json:"vt"` // value type: str|num|bool|json|env|date|bin|msg|flow|global|jsonata
	// HasV records whether the v key was present in the JSON document.
	// Node-RED's migrated flows carry entries without v
	// ({"p":"payload"} / {"p":"topic","vt":"str"}) meaning "use the
	// legacy top-level payload/payloadType/topic fields" — an absent
	// v must be distinguishable from an explicit empty one.
	HasV bool `json:"-"`
}

// UnmarshalJSON decodes the entry and records the presence of the v
// key for the legacy-fallback semantics documented on InjectProp.
func (p *InjectProp) UnmarshalJSON(data []byte) error {
	type alias InjectProp
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*p = InjectProp(a)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	_, p.HasV = raw["v"]
	return nil
}

// InjectNode is a trigger that fires a configured payload once on
// deploy and/or repeatedly on an interval. Injects are fire-and-forget:
// no response is expected, so each message's pending entry is discarded
// right after injection (InjectMessage registers a pending channel
// keyed by msgID that nothing would otherwise resolve).
type InjectNode struct {
	*nodebase.BaseNode
	config          *InjectConfig
	host            nodebase.NodeHost
	workflowID      string
	stopCh          chan struct{}
	stopOnce        sync.Once
	payload         nodestore.MessageInterface // pre-parsed payload template
	payloadType     string
	payloadTemplate string
	propMetadata    map[string]interface{} // extra props entries → metadata
	topic           string                 // resolved topic (config or props)
	localSchedule   *injectSchedule        // schedule slot used when no host is wired
	localOnce       sync.Once
}

// injectSchedule holds the per-run firing schedule. It lives in the
// host's node-state slot because executors may be rebuilt between
// deliveries: a re-executed inject sees `started` and does not
// double-arm its timers.
type injectSchedule struct {
	mu      sync.Mutex
	started bool
	stopped bool
	timers  []*time.Timer
}

// addTimer registers a scheduled timer; when the schedule is already
// stopped the timer is cancelled immediately.
func (s *injectSchedule) addTimer(t *time.Timer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		t.Stop()
		return
	}
	s.timers = append(s.timers, t)
}

// buildPayload materializes the message Data per the configured
// payload type. Called per fire so "timestamp" reflects the fire time,
// not the deploy time (Node-RED behavior).
func (n *InjectNode) buildPayload() nodestore.MessageInterface {
	var data interface{}
	switch n.payloadType {
	case "timestamp", "date":
		data = float64(time.Now().UnixMilli())
	case "string", "str":
		data = n.payloadTemplate
	case "num":
		if f, err := strconv.ParseFloat(strings.TrimSpace(n.payloadTemplate), 64); err == nil {
			data = f
		} else {
			data = float64(0)
		}
	case "bool":
		data = strings.TrimSpace(n.payloadTemplate) == "true"
	case "env":
		data = os.Getenv(n.payloadTemplate)
	default: // "json"
		if err := json.Unmarshal([]byte(n.payloadTemplate), &data); err != nil || data == nil {
			data = "hello"
		}
	}
	metadata := map[string]interface{}{"origin": "inject"}
	if n.topic != "" {
		metadata["topic"] = n.topic
	}
	for k, v := range n.propMetadata {
		metadata[k] = v
	}
	return nodestore.NewMessage().SetPayload(data).SetMetadata(metadata).SetTimestamp(time.Now())
}

// NewInjectNode creates a new inject node
func NewInjectNode(node nodestore.NodeInterface, config *InjectConfig, host nodebase.NodeHost) *InjectNode {
	return &InjectNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
		stopCh:     make(chan struct{}),
	}
}

// Execute parses the configured payload once, then starts the firing
// goroutines (once-shot and/or interval ticker).
func (n *InjectNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.payloadTemplate = n.config.Payload
	n.payloadType = n.config.PayloadType
	n.propMetadata = nil
	n.topic = n.config.Topic
	// Node-RED props array takes precedence over the legacy
	// payload/payloadType/topic fields when present. An entry without
	// a v key means "use the legacy top-level field" — Node-RED writes
	// props this way for migrated pre-props flows.
	for _, p := range n.config.Props {
		switch p.P {
		case "payload":
			if p.HasV {
				n.payloadTemplate = p.V
				n.payloadType = p.VT
			}
		case "topic":
			if p.HasV {
				n.topic = p.V
			}
		default:
			// custom message property → metadata
			if n.propMetadata == nil {
				n.propMetadata = map[string]interface{}{}
			}
			n.propMetadata[p.P] = propValue(p.VT, p.V)
		}
	}
	if n.payloadType == "" {
		if n.payloadTemplate == "" {
			n.payloadType = "timestamp"
		} else {
			n.payloadType = "json"
		}
	}
	n.payload = n.buildPayload()

	// Arm the firing schedule. The armed flag lives in run state so a
	// rebuilt executor does not double-arm timers on a later Execute.
	sched := n.schedule()
	sched.mu.Lock()
	already := sched.started
	if !already {
		sched.started = true
	}
	sched.mu.Unlock()
	if already {
		return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "armed"}).SetTimestamp(time.Now()), nil
	}

	// Node-RED semantics: with once set the repeat schedule is armed
	// inside the once-delay timeout — the first repeat tick is
	// measured from the delayed initial fire, not from deploy.
	startRepeater := func() {
		if period := n.repeatInterval(); period > 0 {
			go n.intervalLoop(ctx, period)
		}
		for _, e := range n.timeEntries() {
			n.armTimeEntry(sched, e)
		}
	}

	if n.config.Once {
		if delay := n.onceDelay(); delay > 0 {
			sched.addTimer(time.AfterFunc(delay, func() {
				n.fire()
				startRepeater()
			}))
		} else {
			go func() {
				n.fire()
				startRepeater()
			}()
		}
	} else {
		startRepeater()
	}

	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "armed"}).SetTimestamp(time.Now()), nil
}

// intervalLoop fires the payload on every tick until stopped or the
// run context is cancelled.
func (n *InjectNode) intervalLoop(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.fire()
		case <-n.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Fire injects a clone of the payload into the workflow — the
// host's FireInjectNode contract (a manual click on the inject
// button). Errors are ignored (e.g. the workflow may be undeploying).
// The pending response channel is discarded immediately — inject
// messages never get a reply.
func (n *InjectNode) Fire() {
	n.fire()
}

// fire injects a clone of the payload into the workflow.
func (n *InjectNode) fire() {
	// Fresh Metadata map per fire: InjectMessage mutates it (msgId)
	metadata := make(map[string]interface{}, len(n.payload.GetMetadata()))
	for k, v := range n.payload.GetMetadata() {
		metadata[k] = v
	}
	built := n.buildPayload()
	payload := nodestore.NewMessage().SetPayload(built.GetPayload()).SetMetadata(metadata).SetTimestamp(time.Now())
	_ = n.host.Emit(n.workflowID, n.GetID(), payload, nil)
}

// propValue coerces an InjectProp value by its vt code. Unknown or
// unsupported types (jsonata, cred) pass through as the raw string.
func propValue(vt, v string) interface{} {
	switch vt {
	case "num":
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
		return float64(0)
	case "bool":
		return strings.TrimSpace(v) == "true"
	case "json":
		var data interface{}
		if err := json.Unmarshal([]byte(v), &data); err == nil {
			return data
		}
		return nil
	case "env":
		return os.Getenv(v)
	case "date":
		return float64(time.Now().UnixMilli())
	default: // str, msg, flow, global, bin, jsonata, cred
		return v
	}
}

// intervalUnit returns the repeat-period multiplier for IntervalUnits
// (default one second).
func (n *InjectNode) intervalUnit() time.Duration {
	switch n.config.IntervalUnits {
	case "min":
		return time.Minute
	case "hr":
		return time.Hour
	case "day":
		return 24 * time.Hour
	}
	return time.Second
}

// schedule returns the per-run firing schedule — the host's state
// slot when a host is wired, a per-executor fallback otherwise
// (unit tests run inject nodes without a host).
func (n *InjectNode) schedule() *injectSchedule {
	if n.host == nil {
		n.localOnce.Do(func() { n.localSchedule = &injectSchedule{} })
		return n.localSchedule
	}
	return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
		return &injectSchedule{}
	}).(*injectSchedule)
}

// onceDelay returns the configured delay before the initial Once
// fire (0 = fire immediately).
func (n *InjectNode) onceDelay() time.Duration {
	if v, ok := nodebase.ParseNumeric(n.config.OnceDelay); ok && v > 0 {
		return time.Duration(v * float64(time.Second))
	}
	return 0
}

// repeatMode resolves which repeat style applies: "interval" (fixed
// period), "times" (time-of-day schedule) or "none". RepeatType is
// the editor's selector; when it is unset or "none" the raw fields
// still win, so imported Node-RED nodes — which carry repeat/crontab
// and no repeatType — schedule the same way Node-RED would run them.
func (n *InjectNode) repeatMode() string {
	switch n.config.RepeatType {
	case "interval":
		return "interval"
	case "times":
		return "times"
	case "none":
		if v, ok := nodebase.ParseNumeric(n.config.Repeat); ok && v > 0 {
			return "interval"
		}
		if strings.TrimSpace(n.config.Crontab) != "" && len(n.config.Times) == 0 {
			return "times"
		}
		return "none"
	}
	if n.config.Interval > 0 {
		return "interval"
	}
	if v, ok := nodebase.ParseNumeric(n.config.Repeat); ok && v > 0 {
		return "interval"
	}
	if len(n.config.Times) > 0 || strings.TrimSpace(n.config.Crontab) != "" {
		return "times"
	}
	return "none"
}

// repeatInterval resolves the fixed repeat period: Interval scaled by
// IntervalUnits, or Node-RED's Repeat field (always seconds).
func (n *InjectNode) repeatInterval() time.Duration {
	if n.repeatMode() != "interval" {
		return 0
	}
	if n.config.Interval > 0 {
		return time.Duration(n.config.Interval * float64(n.intervalUnit()))
	}
	if v, ok := nodebase.ParseNumeric(n.config.Repeat); ok && v > 0 {
		return time.Duration(v * float64(time.Second))
	}
	return 0
}

// timeEntry is a resolved time-of-day schedule entry.
type timeEntry struct {
	hour, min, sec int
	days           map[int]bool // weekday filter (time.Weekday); empty = every day
}

// timeEntries resolves the "at a specific time" schedule: the Times
// list first, then Node-RED's Crontab field (both best-effort —
// unparseable entries are skipped). Capped at three entries like
// Node-RED's UI.
func (n *InjectNode) timeEntries() []timeEntry {
	if n.repeatMode() != "times" {
		return nil
	}
	var out []timeEntry
	for _, t := range n.config.Times {
		e, ok := parseTimeOfDay(t.Time)
		if !ok {
			continue
		}
		e.days = daySet(t.Days)
		out = append(out, e)
	}
	if len(out) == 0 {
		out = parseCrontab(n.config.Crontab)
	}
	if len(out) > 3 {
		out = out[:3]
	}
	return out
}

// armTimeEntry schedules the next fire of a time-of-day entry; the
// timer re-arms itself after each fire so the entry repeats daily on
// its allowed weekdays.
func (n *InjectNode) armTimeEntry(sched *injectSchedule, e timeEntry) {
	wait := nextTimeOfDay(time.Now(), e)
	sched.addTimer(time.AfterFunc(wait, func() {
		n.fire()
		n.armTimeEntry(sched, e)
	}))
}

// parseTimeOfDay parses "HH:MM" or "HH:MM:SS" (24h) into a timeEntry.
func parseTimeOfDay(s string) (timeEntry, bool) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) < 2 || len(parts) > 3 {
		return timeEntry{}, false
	}
	h, errH := strconv.Atoi(parts[0])
	m, errM := strconv.Atoi(parts[1])
	sec := 0
	if len(parts) == 3 {
		v, err := strconv.Atoi(parts[2])
		if err != nil {
			return timeEntry{}, false
		}
		sec = v
	}
	if errH != nil || errM != nil || h < 0 || h > 23 || m < 0 || m > 59 || sec < 0 || sec > 59 {
		return timeEntry{}, false
	}
	return timeEntry{hour: h, min: m, sec: sec}, true
}

// daySet converts a weekday list (0=Sunday … 6=Saturday) into a
// lookup set. Empty or all-seven selections mean "every day".
func daySet(days []int) map[int]bool {
	if len(days) == 0 {
		return nil
	}
	set := map[int]bool{}
	for _, d := range days {
		if d >= 0 && d <= 6 {
			set[d] = true
		}
	}
	if len(set) == 7 {
		return nil
	}
	return set
}

// parseCrontab maps Node-RED's crontab field onto time-of-day
// entries. Each expression is five fields ("M H DOM MON DOW") or six
// with a leading seconds field; multiple expressions are split by
// newline or ";". Day-of-month and month fields must be "*" (the
// schedule is time-of-day based); minute/hour accept numbers, lists,
// ranges and steps; the weekday field accepts the same or "*".
// Anything else is skipped — a full cron engine is out of scope.
func parseCrontab(expr string) []timeEntry {
	var out []timeEntry
	for _, line := range strings.FieldsFunc(expr, func(r rune) bool { return r == '\n' || r == ';' }) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		sec := 0
		if len(fields) == 6 {
			if v, ok := cronSingle(fields[0]); ok {
				sec = v
			}
			fields = fields[1:]
		}
		if len(fields) != 5 || fields[2] != "*" || fields[3] != "*" {
			continue
		}
		mins := cronNumbers(fields[0], 0, 59)
		hours := cronNumbers(fields[1], 0, 23)
		if len(mins) != 1 || len(hours) == 0 {
			continue
		}
		days := daySet(cronNumbers(fields[4], 0, 6))
		for _, h := range hours {
			out = append(out, timeEntry{hour: h, min: mins[0], sec: sec, days: days})
		}
	}
	return out
}

// cronSingle parses a single-number cron field.
func cronSingle(field string) (int, bool) {
	v, err := strconv.Atoi(field)
	if err != nil || v < 0 || v > 59 {
		return 0, false
	}
	return v, true
}

// cronNumbers expands a cron field ("*", "n", "a,b,c", "a-b",
// "a-b/s", "*/s") into its values within [lo,hi]. 7 in a weekday
// field is Sunday, same as 0.
func cronNumbers(field string, lo, hi int) []int {
	seen := map[int]bool{}
	var out []int
	add := func(v int) {
		if v == hi+1 && lo == 0 && hi == 6 {
			v = 0 // weekday "7" is Sunday
		}
		if v >= lo && v <= hi && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		step := 1
		if i := strings.Index(part, "/"); i >= 0 {
			if s, err := strconv.Atoi(part[i+1:]); err == nil && s > 0 {
				step = s
			}
			part = part[:i]
		}
		a, b := lo, hi
		switch {
		case part == "*" || part == "":
			// full range
		case strings.Contains(part, "-"):
			bounds := strings.SplitN(part, "-", 2)
			v1, e1 := strconv.Atoi(bounds[0])
			v2, e2 := strconv.Atoi(bounds[1])
			if e1 != nil || e2 != nil {
				continue
			}
			a, b = v1, v2
		default:
			v, err := strconv.Atoi(part)
			if err != nil {
				continue
			}
			add(v)
			continue
		}
		for v := a; v <= b; v += step {
			add(v)
		}
	}
	return out
}

// nextTimeOfDay returns how long until the entry next fires: the
// first allowed weekday whose HH:MM:SS is still ahead of `now`,
// searching up to a week out.
func nextTimeOfDay(now time.Time, e timeEntry) time.Duration {
	for i := 0; i < 8; i++ {
		day := now.AddDate(0, 0, i)
		if len(e.days) > 0 && !e.days[int(day.Weekday())] {
			continue
		}
		cand := time.Date(day.Year(), day.Month(), day.Day(), e.hour, e.min, e.sec, 0, now.Location())
		if cand.After(now) {
			return cand.Sub(now)
		}
	}
	// No allowed weekday inside the next week — retry tomorrow.
	return 24 * time.Hour
}

// Stop halts the interval ticker and every scheduled timer
// (idempotent)
func (n *InjectNode) Stop() error {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		s := n.schedule()
		s.mu.Lock()
		s.stopped = true
		for _, t := range s.timers {
			t.Stop()
		}
		s.timers = nil
		s.mu.Unlock()
	})
	return nil
}
