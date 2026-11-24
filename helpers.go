package nodebase

import (
	"encoding/json"
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

// ClonePayload deep-copies a payload for a fan-out branch: Payload and
// Metadata are copied via JSON round-trip (so sibling branches can't
// share maps — nested maps like parts would otherwise race when a
// node mutates them on an async emit while the engine journals
// another clone). A clone is a new message instance: it gets a fresh
// envelope ID and records the producing instance's ID in
// metadata.parentMsgId.
func ClonePayload(p nodestore.MessageInterface) nodestore.MessageInterface {
	if p == nil {
		return nil
	}
	clone := messageWithError(nodestore.NewMessage().SetID(GenerateID()), p.GetError()).SetTimestamp(p.GetTimestamp())
	if p.GetPayload() != nil {
		if b, ok := p.GetPayload().([]byte); ok {
			// Binary payloads (fileIn binary mode, submitIn uploads)
			// must survive the edge intact — the JSON round-trip below
			// would otherwise turn them into base64 strings.
			clone.SetPayload(append([]byte(nil), b...))
		} else {
			raw, err := json.Marshal(p.GetPayload())
			if err == nil {
				var data interface{}
				if json.Unmarshal(raw, &data) == nil {
					clone.SetPayload(data)
				}
			}
			if clone.GetPayload() == nil {
				clone.SetPayload(p.GetPayload()) // non-JSON data: share rather than drop
			}
		}
	}
	if p.GetMetadata() != nil {
		if raw, err := json.Marshal(p.GetMetadata()); err == nil {
			var md map[string]interface{}
			if json.Unmarshal(raw, &md) == nil {
				clone.SetMetadata(md)
			}
		}
		if clone.GetMetadata() == nil {
			// Non-JSON metadata: shallow-copy rather than drop.
			clone.SetMetadata(make(map[string]interface{}, len(p.GetMetadata())))
			for k, v := range p.GetMetadata() {
				clone.GetMetadata()[k] = v
			}
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
