// Package split contains the split node implementation.
package split

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"reflect"
	"strconv"
	"strings"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(SplitConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewSplitNode(n, c), nil
}

// init registers the split node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "split",
			Name:        "Split",
			Description: "Splits a string, array or object into a sequence of messages",
			Icon:        "cut",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the split node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Field to split; \"payload\" (default) or a dotted path inside the payload",
			"default":     "payload",
		},
		"splt": map[string]interface{}{
			"type":        "string",
			"title":       "Split Using",
			"description": "Delimiter for strings (escape sequences like \\n allowed) or chunk length when spltType is \"len\"",
			"default":     "\\n",
		},
		"spltType": map[string]interface{}{
			"type":    "string",
			"title":   "Split Type",
			"enum":    []string{"str", "bin", "len"},
			"default": "str",
		},
		"arraySplt": map[string]interface{}{
			"type":        "integer",
			"title":       "Array Chunk Length",
			"description": "Number of array elements per emitted message",
			"default":     1,
		},
		"addname": map[string]interface{}{
			"type":        "string",
			"title":       "Add Name",
			"description": "Optional field name on each output message receiving the part's key (objects) or index",
		},
	},
}

// SplitConfig holds configuration for the split node — the Node-RED
// split equivalent. Splits a string, array, object or buffer into a
// sequence of messages tagged with `parts` metadata so a downstream
// join node can reassemble them.
type SplitConfig struct {
	// Property is the message field to split; "payload" (default) targets
	// msg.Payload, otherwise a dotted path inside the data map
	Property string `json:"property"`
	// Splt is the split delimiter (strings/buffers) or the fixed record
	// length when SpltType is "len" (parseInt semantics apply)
	Splt string `json:"splt"`
	// SpltType selects the split mode: "str" (default) | "bin" | "len"
	SpltType string `json:"spltType"`
	// ArraySplt is the chunk length for array payloads (default 1)
	ArraySplt int `json:"arraySplt"`
	// Addname optionally copies each part's key (objects) or index to a
	// field on every emitted message
	Addname string `json:"addname"`
}

// SplitNode expands one message into a sequence. Execute is a
// pass-through; the actual fan-out happens in Expand, driven by the
// host's MessageExpander hook.
type SplitNode struct {
	*nodebase.BaseNode
	config *SplitConfig
}

// NewSplitNode creates a new split node
func NewSplitNode(node nodestore.NodeInterface, config *SplitConfig) *SplitNode {
	return &SplitNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute passes the input through unchanged — expansion is done by the
// host via Expand
func (n *SplitNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// Expand splits the configured property into a sequence of payloads,
// each carrying `parts` metadata (id, type, index, count, plus ch for
// strings, key for objects, property when not "payload"). A previous
// `parts` block is preserved under parts.parts (nested splits).
// Non-splittable data yields the original message unchanged.
func (n *SplitNode) Expand(output nodestore.MessageInterface) []nodestore.MessageInterface {
	if output == nil {
		return nil
	}

	sourceProp := n.property()
	value, onData := splitSource(output, sourceProp)

	var chunks []interface{}
	var partType string
	var keys []string // object keys, nil otherwise
	var delim string  // split delimiter, strings/buffers only

	switch v := value.(type) {
	case string:
		partType = "string"
		if n.spltType() == "len" {
			chunks = splitStringByLen(v, parseIntLike(n.spltValue(), 1))
		} else {
			delim = nodebase.UnescapeDelim(n.spltValue())
			chunks = splitStringByDelim(v, delim)
		}
	case []byte:
		partType = "buffer"
		if n.spltType() == "len" {
			chunks = splitBytesByLen(v, parseIntLike(n.spltValue(), 1))
		} else {
			delim = nodebase.UnescapeDelim(n.spltValue())
			chunks = splitBytesByDelim(v, []byte(delim))
		}
	case map[string]interface{}:
		partType = "object"
		// Sorted keys: Go maps are unordered, so Node-RED's insertion
		// order is unavailable — sorted order is a documented deviation.
		keys = nodebase.SortedKeys(v)
		for _, k := range keys {
			chunks = append(chunks, v[k])
		}
	default:
		// Any slice/array type (e.g. []string, []int) splits into
		// fixed-size chunks of ArraySplt elements (default 1)
		rv := reflect.ValueOf(value)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			partType = "array"
			size := n.config.ArraySplt
			if size <= 0 {
				size = 1
			}
			for i := 0; i < rv.Len(); i += size {
				end := i + size
				if end > rv.Len() {
					end = rv.Len()
				}
				if size == 1 {
					// Node-RED emits the element itself when the chunk
					// length is 1, not a one-element array.
					chunks = append(chunks, rv.Index(i).Interface())
					continue
				}
				chunk := make([]interface{}, 0, end-i)
				for j := i; j < end; j++ {
					chunk = append(chunk, rv.Index(j).Interface())
				}
				chunks = append(chunks, chunk)
			}
			break
		}
		return []nodestore.MessageInterface{output} // not splittable
	}

	if len(chunks) == 0 {
		return nil // empty input: nothing to emit
	}

	groupID := fmt.Sprintf("parts-%d", nodebase.NextGroupSeq())
	count := len(chunks)

	out := make([]nodestore.MessageInterface, 0, count)
	for i, chunk := range chunks {
		p := nodebase.ClonePayload(output)
		if p.GetMetadata() == nil {
			p.SetMetadata(map[string]interface{}{})
		}

		// Assign the chunk back at the source location
		if onData {
			p.SetPayload(chunk)
		} else if m, ok := p.GetPayload().(map[string]interface{}); ok {
			nodebase.SetPath(m, sourceProp, chunk)
		}

		parts := map[string]interface{}{
			"id":    groupID,
			"type":  partType,
			"index": i,
			"count": count,
		}
		if partType == "array" {
			// Node-RED records the configured chunk length so a
			// downstream join can re-flatten multi-element chunks.
			size := n.config.ArraySplt
			if size > 0 {
				parts["len"] = size
			}
		}
		if delim != "" {
			parts["ch"] = delim
		}
		if keys != nil {
			parts["key"] = keys[i]
		}
		if !onData {
			// Node-RED records the full message path (payload.*) so a
			// downstream join can read and write back the same property.
			parts["property"] = "payload." + sourceProp
		}
		if prev, ok := p.GetMetadata()["parts"]; ok {
			parts["parts"] = prev // preserve outer split's parts
		}
		p.GetMetadata()["parts"] = parts

		// addname: copy the part's key (objects) or index onto the message
		if n.config.Addname != "" {
			var nameVal interface{} = i
			if keys != nil {
				nameVal = keys[i]
			}
			if m, ok := p.GetPayload().(map[string]interface{}); ok {
				m[n.config.Addname] = nameVal
			} else {
				p.GetMetadata()[n.config.Addname] = nameVal
			}
		}

		out = append(out, p)
	}
	return out
}

// property returns the configured source property (default "payload")
func (n *SplitNode) property() string {
	if n.config.Property == "" {
		return "payload"
	}
	return n.config.Property
}

// spltType returns the split mode (default "str")
func (n *SplitNode) spltType() string {
	if n.config.SpltType == "len" || n.config.SpltType == "bin" {
		return n.config.SpltType
	}
	return "str"
}

// spltValue returns the configured delimiter/length (default "\n")
func (n *SplitNode) spltValue() string {
	if n.config.Splt == "" {
		return "\n"
	}
	return n.config.Splt
}

// splitSource resolves the value to split. When property is "payload"
// (or empty) the whole Data is split; otherwise the dotted path is read
// inside the data map. Returns the value plus whether it lives on Data.
func splitSource(output nodestore.MessageInterface, property string) (interface{}, bool) {
	if property == "payload" {
		return output.GetPayload(), true
	}
	if m, ok := output.GetPayload().(map[string]interface{}); ok {
		if v, found := nodebase.GetPath(m, property); found {
			return v, false
		}
	}
	return nil, false
}

// parseIntLike mimics JavaScript's parseInt: leading optional sign and
// digits are read; decimals are truncated; invalid input returns the
// fallback (minimum 1).
func parseIntLike(s string, fallback int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return fallback
	}
	i := 0
	if s[0] == '+' || s[0] == '-' {
		i++
	}
	start := i
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == start {
		// No leading digits — try a truncated decimal ("3.9" → 3)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			n := int(f)
			if n > 0 {
				return n
			}
		}
		return fallback
	}
	n, err := strconv.Atoi(s[:i])
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// splitStringByDelim splits s on the delimiter. An empty delimiter
// splits into individual characters.
func splitStringByDelim(s, delim string) []interface{} {
	if delim == "" {
		chunks := make([]interface{}, 0, len(s))
		for _, r := range s {
			chunks = append(chunks, string(r))
		}
		return chunks
	}
	pieces := strings.Split(s, delim)
	chunks := make([]interface{}, 0, len(pieces))
	for _, p := range pieces {
		chunks = append(chunks, p)
	}
	return chunks
}

// splitStringByLen splits s into fixed-length chunks (rune-aware)
func splitStringByLen(s string, size int) []interface{} {
	if size <= 0 {
		size = 1
	}
	runes := []rune(s)
	var chunks []interface{}
	for i := 0; i < len(runes); i += size {
		end := i + size
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[i:end]))
	}
	return chunks
}

// splitBytesByDelim splits b on the delimiter bytes
func splitBytesByDelim(b, delim []byte) []interface{} {
	if len(delim) == 0 {
		chunks := make([]interface{}, 0, len(b))
		for _, c := range b {
			chunks = append(chunks, []byte{c})
		}
		return chunks
	}
	pieces := bytes.Split(b, delim)
	chunks := make([]interface{}, 0, len(pieces))
	for _, p := range pieces {
		chunks = append(chunks, p)
	}
	return chunks
}

// splitBytesByLen splits b into fixed-length byte chunks
func splitBytesByLen(b []byte, size int) []interface{} {
	if size <= 0 {
		size = 1
	}
	var chunks []interface{}
	for i := 0; i < len(b); i += size {
		end := i + size
		if end > len(b) {
			end = len(b)
		}
		chunk := make([]byte, end-i)
		copy(chunk, b[i:end])
		chunks = append(chunks, chunk)
	}
	return chunks
}
