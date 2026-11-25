package nodebase

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	nodestore "github.com/dracory/nodestore"
)

// Message-property access matching Node-RED's getMessageProperty /
// setMessageProperty semantics: the message is a flat object where
// "payload" (and "payload.x") resolves into the payload, and every
// other path resolves into message metadata (msg.topic, msg.parts,
// msg.complete, …).

// MessageMap exposes a message as a Node-RED-style plain object —
// every metadata key plus "payload" — for expression evaluation.
func MessageMap(input nodestore.MessageInterface) map[string]interface{} {
	m := map[string]interface{}{}
	for k, v := range input.GetMetadata() {
		m[k] = v
	}
	m["payload"] = input.GetPayload()
	return m
}

// NodeProperty resolves a Node-RED message property path: "payload"
// returns the payload, "payload.x" walks inside it, anything else
// resolves as a dotted path inside metadata.
func NodeProperty(input nodestore.MessageInterface, path string) (interface{}, bool) {
	if input == nil || path == "" {
		return nil, false
	}
	if path == "payload" {
		return input.GetPayload(), true
	}
	if rest, ok := strings.CutPrefix(path, "payload."); ok {
		if m, ok := input.GetPayload().(map[string]interface{}); ok {
			return GetPath(m, rest)
		}
		return nil, false
	}
	if md := input.GetMetadata(); md != nil {
		return GetPath(md, path)
	}
	return nil, false
}

// NodeSetProperty writes a Node-RED message property path: "payload"
// replaces the payload, "payload.x" writes inside it, anything else
// writes a dotted path inside metadata.
func NodeSetProperty(input nodestore.MessageInterface, path string, value interface{}) {
	if path == "payload" {
		input.SetPayload(value)
		return
	}
	if rest, ok := strings.CutPrefix(path, "payload."); ok {
		m, ok := input.GetPayload().(map[string]interface{})
		if !ok {
			m = map[string]interface{}{}
			input.SetPayload(m)
		}
		SetPath(m, rest, value)
		return
	}
	if input.GetMetadata() == nil {
		input.SetMetadata(map[string]interface{}{})
	}
	SetPath(input.GetMetadata(), path, value)
}

// JSString renders a value the way JavaScript's String() / template
// coercion does: numbers drop trailing ".0", nil becomes "" (the
// Array.join convention — null/undefined join as empty), objects and
// arrays become compact JSON, everything else uses %v.
func JSString(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return jsNumber(x)
	case float32:
		return jsNumber(float64(x))
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case json.Number:
		return x.String()
	case map[string]interface{}, []interface{}:
		if raw, err := json.Marshal(v); err == nil {
			return string(raw)
		}
	}
	return fmt.Sprintf("%v", v)
}

// jsNumber formats a float like JavaScript's Number→String: integral
// values print without a decimal point.
func jsNumber(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e21 {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// JSNumber coerces a value like JavaScript's Number(): strings parse
// (empty → 0), bools become 0/1, nil becomes 0. ok is false when the
// coercion yields NaN.
func JSNumber(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, true
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	f, err := ToFloat(v)
	if err != nil {
		return 0, false
	}
	return f, true
}

// JSCompare orders two values with JavaScript's > semantics: two
// strings compare lexically; otherwise both sides coerce through
// Number() (NaN makes the comparison equal-ish — it returns -1 like
// a failed > does through the caller). Returns -1, 0 or 1.
func JSCompare(a, b interface{}) int {
	_, aStr := a.(string)
	_, bStr := b.(string)
	if aStr && bStr {
		as, bs := a.(string), b.(string)
		switch {
		case as < bs:
			return -1
		case as > bs:
			return 1
		}
		return 0
	}
	fa, okA := JSNumber(a)
	fb, okB := JSNumber(b)
	if !okA || !okB {
		// NaN comparisons: "x > y" is false and "x === y" is false,
		// which falls through to -dir in the Node-RED comparator.
		return -1
	}
	switch {
	case fa < fb:
		return -1
	case fa > fb:
		return 1
	}
	return 0
}
