package nodebase

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	nodestore "github.com/dracory/nodestore"
)

// Switch-rule evaluation shared by the switch and condition nodes
// (condition builds a single eq/neq rule and delegates to the same
// machinery).

// EvalSwitchRule evaluates a single rule against the payload's data —
// the host-less variant kept for callers that never reference context.
func EvalSwitchRule(rule SwitchRule, payload nodestore.MessageInterface) bool {
	return EvalSwitchRuleCtx(rule, payload, nil)
}

// EvalSwitchRuleCtx is EvalSwitchRule with the host seam attached:
// rules whose property or comparison value reads flow/global context
// or an environment variable resolve through rc.
func EvalSwitchRuleCtx(rule SwitchRule, payload nodestore.MessageInterface, rc *RuleContext) bool {
	var value interface{}
	found := false
	switch {
	case rule.PropertyType == "flow" || rule.PropertyType == "global":
		value, found = rc.ContextGet(rule.PropertyType, rule.Property)
	case rule.PropertyType == "jsonata":
		// The switch value is a JSONata expression evaluated against
		// the whole message (Node-RED propertyType "jsonata").
		if payload != nil {
			if v, err := EvalJSONataCtx(rule.Property, payload, rc); err == nil && v != nil {
				value, found = v, true
			}
		}
	case payload != nil:
		if rule.Property == "" {
			// Node-RED parity: "msg.payload" evaluates the payload
			// itself — works for scalar payloads too.
			value, found = payload.GetPayload(), true
		} else if m, ok := payload.GetPayload().(map[string]interface{}); ok {
			value, found = GetPath(m, rule.Property)
		}
		if !found {
			// Node-RED msg.<prop> can point outside payload (topic,
			// parts, …) — fall back to message metadata.
			value, found = GetPath(payload.GetMetadata(), rule.Property)
		}
	}

	expected := rule.expectedValue(payload, rc)

	switch rule.Op {
	case "eq":
		return found && switchEqual(value, expected)
	case "neq":
		return !found || !switchEqual(value, expected)
	case "gt":
		return found && switchCompare(value, expected) > 0
	case "lt":
		return found && switchCompare(value, expected) < 0
	case "lte":
		return found && switchCompare(value, expected) <= 0
	case "gte":
		return found && switchCompare(value, expected) >= 0
	case "contains":
		return found && switchContains(value, expected)
	case "empty":
		return !found || isEmptyValue(value)
	case "nempty":
		return found && !isEmptyValue(value)
	case "regex":
		return found && switchRegex(value, rule.Value)
	case "istype":
		return switchType(value, found, rule.Value)
	case "true":
		b, ok := value.(bool)
		return found && ok && b
	case "false":
		b, ok := value.(bool)
		return found && ok && !b
	case "null":
		// A missing property is "undefined", not null — like
		// Node-RED the rule does not match in that case.
		return found && value == nil
	case "nnull":
		return !found || value != nil
	case "else":
		// Catch-all: the standalone rule always matches. The switch
		// node gives it real semantics — an else output only receives
		// messages no other rule claimed.
		return true
	case "jsonata_exp":
		// Node-RED's jsonata_exp: the rule value is a JSONata
		// expression evaluated against the message; the rule matches
		// iff the result is exactly true.
		b, ok := expected.(bool)
		return ok && b
	case "btwn":
		// Inclusive range check against Value and Value2.
		return found &&
			switchCompare(value, expected) >= 0 &&
			switchCompare(value, rule.expectedValue2(payload, rc)) <= 0
	case "hask":
		// The resolved property is an object that has the named key.
		if m, ok := value.(map[string]interface{}); ok && found {
			_, has := m[rule.Value]
			return has
		}
		return false
	case "head":
		// Sequence op: parts.index within the first Value members.
		index, _, ok := switchParts(payload)
		n, err := ToFloat(expected)
		return ok && err == nil && float64(index) < n
	case "tail":
		// Sequence op: parts.index within the last Value members.
		index, count, ok := switchParts(payload)
		n, err := ToFloat(expected)
		return ok && err == nil && float64(index) >= float64(count)-n
	case "index":
		// Sequence op: parts.index inside the [Value, Value2] range.
		index, _, ok := switchParts(payload)
		if !ok {
			return false
		}
		v1, err1 := ToFloat(expected)
		v2, err2 := ToFloat(rule.expectedValue2(payload, rc))
		return err1 == nil && err2 == nil &&
			float64(index) >= v1 && float64(index) <= v2
	}
	return false
}

// switchParts extracts msg.parts sequence metadata for the head, tail
// and index operators. ok is false when the message carries no usable
// parts.index (not part of a sequence).
func switchParts(payload nodestore.MessageInterface) (index, count int, ok bool) {
	if payload == nil {
		return 0, 0, false
	}
	parts, isMap := payload.GetMetadata()["parts"].(map[string]interface{})
	if !isMap {
		return 0, 0, false
	}
	idx, err := ToFloat(parts["index"])
	if err != nil {
		return 0, 0, false
	}
	cnt, _ := ToFloat(parts["count"])
	return int(idx), int(cnt), true
}

// switchRegex matches the value's string form against a regular
// expression (leading/trailing slashes in the rule are optional).
func switchRegex(value interface{}, pattern string) bool {
	p := strings.TrimPrefix(strings.TrimSuffix(pattern, "/"), "/")
	re, err := regexp.Compile(p)
	if err != nil {
		return false
	}
	return re.MatchString(fmt.Sprintf("%v", value))
}

// switchType implements the Node-RED "istype" operator for the common
// JSON types: string | number | boolean | array | object | null.
func switchType(value interface{}, found bool, expected string) bool {
	if !found {
		return expected == "null" || expected == "undefined"
	}
	switch expected {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		switch value.(type) {
		case float64, float32, int, int64, int32, json.Number:
			return true
		}
		return false
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		_, ok := value.([]interface{})
		return ok
	case "object":
		_, ok := value.(map[string]interface{})
		return ok
	case "null":
		return value == nil
	}
	return false
}

// switchEqual compares a payload value with a rule value. When both
// sides parse as floats they compare numerically; otherwise they
// compare their string forms.
func switchEqual(value, expected interface{}) bool {
	// Booleans compare strictly — Node-RED's typedInput bool must not
	// match the string "true"/"false".
	if be, ok := expected.(bool); ok {
		bv, ok2 := value.(bool)
		return ok2 && bv == be
	}
	if bv, ok := value.(bool); ok {
		be, ok2 := expected.(bool)
		return ok2 && bv == be
	}
	if fv, err1 := ToFloat(value); err1 == nil {
		if fe, err2 := ToFloat(expected); err2 == nil {
			return fv == fe
		}
	}
	return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", expected)
}

// switchCompare returns -1/0/1 comparing value against expected.
// Numeric compare when both parse as floats, else string compare.
func switchCompare(value, expected interface{}) int {
	if fv, err1 := ToFloat(value); err1 == nil {
		if fe, err2 := ToFloat(expected); err2 == nil {
			switch {
			case fv > fe:
				return 1
			case fv < fe:
				return -1
			}
			return 0
		}
	}
	s := fmt.Sprintf("%v", value)
	e := fmt.Sprintf("%v", expected)
	switch {
	case s > e:
		return 1
	case s < e:
		return -1
	}
	return 0
}

// switchContains checks substring membership for strings and element
// membership for arrays.
func switchContains(value, expected interface{}) bool {
	switch v := value.(type) {
	case string:
		return strings.Contains(v, fmt.Sprintf("%v", expected))
	case []interface{}:
		for _, item := range v {
			if switchEqual(item, expected) {
				return true
			}
		}
	}
	return false
}

// isEmptyValue reports whether a payload value counts as empty:
// nil, empty string, or an empty array/map.
func isEmptyValue(value interface{}) bool {
	if value == nil {
		return true
	}
	if s, ok := value.(string); ok {
		return s == ""
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Map, reflect.Array:
		return rv.Len() == 0
	}
	return false
}

// SwitchRule is a single ordered rule evaluated against the incoming
// payload. Property is a dotted path into msg.Payload (e.g. "path" or
// "body.user.role") — or a context key when PropertyType is
// "flow"/"global", or a sequence expression evaluated against the
// whole message when PropertyType is "jsonata". Rule index i maps to
// output port "output_{i+1}".
// ValueType is Node-RED's vt: how Value is interpreted — "str"
// (default), "num", "bool", "json", "msg" (a message property path),
// "env" (environment variable name) or "flow"/"global" (context key).
// Value2/Value2Type hold the second comparison value (Node-RED's
// v2/vt2) used by the range ops "btwn" and "index".
type SwitchRule struct {
	Property     string `json:"property"`
	PropertyType string `json:"propertyType,omitempty"` // ""|"msg" (default) | "flow" | "global"
	Op           string `json:"op"`                     // eq | neq | gt | lt | lte | gte | contains | empty | nempty | regex | istype | true | false | null | nnull | else | btwn | hask | head | tail | index
	Value        string `json:"value"`
	ValueType    string `json:"valueType,omitempty"`
	Value2       string `json:"value2,omitempty"`
	Value2Type   string `json:"value2Type,omitempty"`
}

// expectedValue resolves the rule's comparison value per ValueType —
// Node-RED's typedInput semantics (v interpreted as vt). Context and
// env lookups go through rc (nil disables them).
func (r SwitchRule) expectedValue(payload nodestore.MessageInterface, rc *RuleContext) interface{} {
	return resolveRuleValue(r.Value, r.ValueType, payload, rc)
}

// expectedValue2 is expectedValue for the second range bound (v2/vt2).
func (r SwitchRule) expectedValue2(payload nodestore.MessageInterface, rc *RuleContext) interface{} {
	return resolveRuleValue(r.Value2, r.Value2Type, payload, rc)
}

// resolveRuleValue applies typedInput semantics to a raw rule value:
// env/flow/global lookups go through rc, num/bool/json coerce the
// string, msg reads a message property, anything else stays a string.
func resolveRuleValue(value, valueType string, payload nodestore.MessageInterface, rc *RuleContext) interface{} {
	switch valueType {
	case "env":
		v, _ := rc.Env(value)
		return v
	case "flow", "global":
		v, _ := rc.ContextGet(valueType, value)
		return v
	case "jsonata":
		// The comparison value is a JSONata expression evaluated
		// against the message — Node-RED's vt:"jsonata" typedInput.
		v, err := EvalJSONataCtx(value, payload, rc)
		if err != nil {
			return nil
		}
		return v
	case "num":
		if f, ok := ParseNumericString(value); ok {
			return f
		}
		return value
	case "bool":
		return value == "true" || value == "1"
	case "json":
		var v interface{}
		if err := json.Unmarshal([]byte(value), &v); err == nil {
			return v
		}
		return value
	case "msg":
		// The comparison value is itself read from the message —
		// "payload"/"payload.x" resolve into Data like Property,
		// anything else hits metadata first, then the payload map.
		if payload == nil {
			return nil
		}
		path := value
		if path == "payload" {
			return payload.GetPayload()
		}
		if rest, ok := strings.CutPrefix(path, "payload."); ok {
			if m, ok := payload.GetPayload().(map[string]interface{}); ok {
				v, _ := GetPath(m, rest)
				return v
			}
			return nil
		}
		if v, ok := GetPath(payload.GetMetadata(), path); ok {
			return v
		}
		if m, ok := payload.GetPayload().(map[string]interface{}); ok {
			v, _ := GetPath(m, path)
			return v
		}
		return nil
	default: // "", "str"
		return value
	}
}
