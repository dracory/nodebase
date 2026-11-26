package nodebase

import (
	"encoding/json"
	"fmt"
	"strings"

	"uuid"

	nodestore "github.com/dracory/nodestore"
)

// Shared helpers used across node implementations and by hosts
// embedding this package's contracts.

// GenerateID returns a fresh unique ID (UUIDv7 — time-ordered, so
// persisted rows sort by creation).
func GenerateID() string {
	return uuid.NewV7().String()
}

// NewID returns a fresh unique ID for workflow elements.
func NewID() string {
	return GenerateID()
}

// StringOf returns v as a string when it is one, "" otherwise.
func StringOf(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// PayloadBytes normalizes a message payload to bytes for byte-oriented
// consumers: strings are their UTF-8 bytes, []byte passes through unchanged,
// and every other JSON-serializable value is encoded as JSON. A nil payload
// therefore becomes the JSON document "null"; consumers for which nil means
// "missing" must reject it before calling this helper.
func PayloadBytes(data any) ([]byte, error) {
	switch v := data.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("payload cannot serialize as JSON: %w", err)
		}
		return raw, nil
	}
}

// ParseLinkList accepts links configured either as a JSON array of node
// ids or as a comma-separated string (the editor's textarea first pass).
func ParseLinkList(v interface{}) []string {
	var links []string
	switch t := v.(type) {
	case string:
		for _, s := range strings.Split(t, ",") {
			if s = strings.TrimSpace(s); s != "" {
				links = append(links, s)
			}
		}
	case []interface{}:
		for _, item := range t {
			if s, ok := item.(string); ok && s != "" {
				links = append(links, s)
			}
		}
	}
	return links
}

// NodeName returns the display name of a workflow node.
func NodeName(node nodestore.NodeInterface) string {
	if name, ok := node.GetData()["name"].(string); ok && name != "" {
		return name
	}
	return node.GetID()
}

// OverflowPolicy selects what happens when a message is offered to a
// node inbox that has reached its configured capacity.
type OverflowPolicy string

const (
	// OverflowBlock parks the message until the node's pending count
	// drops below capacity.
	OverflowBlock OverflowPolicy = "block"
	// OverflowDropNewest discards the incoming message.
	OverflowDropNewest OverflowPolicy = "drop-newest"
	// OverflowDropOldest evicts the node's oldest queued message to
	// make room for the incoming one.
	OverflowDropOldest OverflowPolicy = "drop-oldest"
	// OverflowRouteToCatch discards the incoming message and reports it
	// to in-scope catch nodes as an error event.
	OverflowRouteToCatch OverflowPolicy = "route-to-catch"
)

// ParseOverflowPolicy normalizes a config value into an OverflowPolicy.
func ParseOverflowPolicy(value interface{}) (OverflowPolicy, bool) {
	s, _ := value.(string)
	switch OverflowPolicy(strings.ToLower(strings.TrimSpace(s))) {
	case OverflowBlock:
		return OverflowBlock, true
	case OverflowDropNewest:
		return OverflowDropNewest, true
	case OverflowDropOldest:
		return OverflowDropOldest, true
	case OverflowRouteToCatch:
		return OverflowRouteToCatch, true
	}
	return "", false
}

// messageWithError sets a *string error on a message built via the
// fluent nodestore.NewMessage() chain.
func messageWithError(m nodestore.MessageInterface, err *string) nodestore.MessageInterface {
	if err != nil {
		m.SetError(*err)
	}
	return m
}

// ClonePayload deep-copies a payload for a fan-out branch: Payload is
// copied through the shared durable payload JSON codec (so sibling branches
// can't share maps and []byte values survive as bytes), Metadata is
// shallow-copied preserving the msgId needed by response
// nodes — shallow deliberately: the JSON round-trip would coerce
// non-JSON-safe types (int64→float64), and nested-map mutations are
// the mutating node's job to detach (see sort.emitSequence). A clone
// is a new message instance: it gets a fresh envelope ID and records
// the producing instance's ID in metadata.parentMsgId.
func ClonePayload(p nodestore.MessageInterface) nodestore.MessageInterface {
	if p == nil {
		return nil
	}
	clone := messageWithError(nodestore.NewMessage().SetID(GenerateID()), p.GetError()).SetTimestamp(p.GetTimestamp())
	if p.GetPayload() != nil {
		raw, err := nodestore.MarshalPayloadJSON(p.GetPayload())
		if err == nil {
			data, err := nodestore.UnmarshalPayloadJSON(raw)
			if err == nil {
				clone.SetPayload(data)
			}
		}
		if clone.GetPayload() == nil {
			clone.SetPayload(p.GetPayload()) // non-JSON data: share rather than drop
		}
	}
	if p.GetMetadata() != nil {
		clone.SetMetadata(make(map[string]interface{}, len(p.GetMetadata())))
		for k, v := range p.GetMetadata() {
			clone.GetMetadata()[k] = v
		}
	}
	if p.GetID() != "" {
		if clone.GetMetadata() == nil {
			clone.SetMetadata(map[string]interface{}{})
		}
		clone.GetMetadata()["parentMsgId"] = p.GetID()
	}
	return clone
}

// cloneMetadata shallow-copies a payload's metadata so the delayed
// timer's payload doesn't share the (later mutated) map.
func CloneMetadata(input nodestore.MessageInterface) map[string]interface{} {
	metadata := map[string]interface{}{}
	if input == nil {
		return metadata
	}
	for k, v := range input.GetMetadata() {
		metadata[k] = v
	}
	return metadata
}
