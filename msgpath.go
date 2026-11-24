package nodebase

import (
	"encoding/json"
	"strings"

	nodestore "github.com/dracory/nodestore"
)

// Message-path helpers shared by the data-transform nodes (change,
// switch, range, rbe, split, join, csv, json, db, exec, file, html):
// dotted paths into a map-typed payload plus the "msg." prefix that
// targets metadata.

// parseJSONOrString returns the decoded JSON value when s parses as
// JSON, otherwise s itself
func ParseJSONOrString(s string) interface{} {
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		return v
	}
	return s
}

// msgPath strips the "msg." prefix that targets message-level
// properties (Metadata), e.g. "msg.statusCode" -> "statusCode".
func MsgPath(field string) (string, bool) {
	rest, ok := strings.CutPrefix(field, "msg.")
	return rest, ok && rest != ""
}

// splitPath splits a dotted field path into non-empty segments
func SplitPath(path string) []string {
	parts := strings.Split(path, ".")
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// setPath sets value at the dotted path inside m, creating nested maps
// as needed
func SetPath(m map[string]interface{}, path string, value interface{}) {
	parts := SplitPath(path)
	if len(parts) == 0 {
		return
	}
	cur := m
	for _, p := range parts[:len(parts)-1] {
		next, ok := cur[p].(map[string]interface{})
		if !ok {
			next = map[string]interface{}{}
			cur[p] = next
		}
		cur = next
	}
	cur[parts[len(parts)-1]] = value
}

// getPath reads the value at the dotted path inside m
func GetPath(m map[string]interface{}, path string) (interface{}, bool) {
	parts := SplitPath(path)
	if len(parts) == 0 {
		return nil, false
	}
	cur := m
	for _, p := range parts[:len(parts)-1] {
		next, ok := cur[p].(map[string]interface{})
		if !ok {
			return nil, false
		}
		cur = next
	}
	v, ok := cur[parts[len(parts)-1]]
	return v, ok
}

// deletePath removes the value at the dotted path inside data
func DeletePath(data interface{}, path string) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return
	}
	parts := SplitPath(path)
	if len(parts) == 0 {
		return
	}
	cur := m
	for _, p := range parts[:len(parts)-1] {
		next, ok := cur[p].(map[string]interface{})
		if !ok {
			return
		}
		cur = next
	}
	delete(cur, parts[len(parts)-1])
}

// lookupProperty resolves a simple property path against the payload:
// "payload" → Data; otherwise checks Metadata[key] first, then a dotted
// path inside a map-typed Data.
func LookupProperty(input nodestore.MessageInterface, path string) (interface{}, bool) {
	if input == nil || path == "" {
		return nil, false
	}
	if path == "payload" {
		return input.GetPayload(), true
	}
	if v, ok := input.GetMetadata()[path]; ok {
		return v, true
	}
	current := input.GetPayload()
	for _, key := range strings.Split(path, ".") {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		current, ok = m[key]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// rangeProperty extracts the watched value: "payload" returns the whole
// Data, other paths read a dotted path inside the data map.
func RangeProperty(input nodestore.MessageInterface, property string) (interface{}, bool) {
	if property == "payload" {
		return input.GetPayload(), input.GetPayload() != nil
	}
	if m, ok := input.GetPayload().(map[string]interface{}); ok {
		return GetPath(m, property)
	}
	return nil, false
}
