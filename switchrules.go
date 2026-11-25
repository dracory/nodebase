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

// evalSwitchRule evaluates a single rule against the payload's data.
func EvalSwitchRule(rule SwitchRule, payload nodestore.MessageInterface) bool {
	var value interface{}
	found := false
	if payload != nil {
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

	expected := rule.expectedValue(payload)

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
	case "regex":
		return found && switchRegex(value, rule.Value)
	case "istype":
		return switchType(value, found, rule.Value)
	}
	return false
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
// "body.user.role"). Rule index i maps to output port "output_{i+1}".
// ValueType is Node-RED's vt: how Value is interpreted — "str"
// (default), "num", "bool", "json" or "msg" (a message property path).
type SwitchRule struct {
	Property  string `json:"property"`
	Op        string `json:"op"` // eq | neq | gt | lt | lte | gte | contains | empty | regex | istype
	Value     string `json:"value"`
	ValueType string `json:"valueType,omitempty"`
}

// expectedValue resolves the rule's comparison value per ValueType —
// Node-RED's typedInput semantics (v interpreted as vt).
func (r SwitchRule) expectedValue(payload nodestore.MessageInterface) interface{} {
	switch r.ValueType {
	case "num":
		if f, ok := ParseNumericString(r.Value); ok {
			return f
		}
		return r.Value
	case "bool":
		return r.Value == "true" || r.Value == "1"
	case "json":
		var v interface{}
		if err := json.Unmarshal([]byte(r.Value), &v); err == nil {
			return v
		}
		return r.Value
	case "msg":
		// The comparison value is itself read from the message —
		// "payload"/"payload.x" resolve into Data like Property,
		// anything else hits metadata first, then the payload map.
		if payload == nil {
			return nil
		}
		path := r.Value
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
	default: // "", "str", "jsonata" (until the JSONata seam lands)
		return r.Value
	}
}
