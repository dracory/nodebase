// Package csv contains the csv node implementation.
package csv

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(CSVConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewCSVNode(n, c), nil
}

// init registers the csv node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "csv",
			Name:        "CSV",
			Description: "Parses CSV into one message per row, or serializes rows to CSV",
			Icon:        "table",
			Category:    "Parsers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the csv node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Field to convert; \"payload\" (default) or a dotted path inside the payload",
			"default":     "payload",
		},
		"headers": map[string]interface{}{
			"type":        "boolean",
			"title":       "First Row is Header",
			"description": "Parse: rows become objects keyed by the header line; serialize: write a header row from object keys",
			"default":     false,
		},
	},
}

// CSVConfig holds configuration for the csv node — a simplified
// Node-RED CSV parser equivalent.
type CSVConfig struct {
	// Property is the message field to convert; "payload" (default)
	// targets msg.Payload, otherwise a dotted path inside the data map
	Property string `json:"property"`
	// Headers treats the first row as a header line: parsed rows become
	// objects keyed by the header names; serializing objects writes a
	// header row derived from the first object's keys
	Headers bool `json:"headers"`
}

// CSVNode converts between a CSV string and row data. A string input is
// parsed and expanded into one message per row (parts metadata, like
// the split node); an array input is serialized into a CSV string.
type CSVNode struct {
	*nodebase.BaseNode
	config *CSVConfig
	// rows holds the parse result between Execute and Expand; nil in
	// serialize mode
	rows [][]string
	// header holds the first row when Headers is set
	header []string
}

// NewCSVNode creates a new csv node
func NewCSVNode(node nodestore.NodeInterface, config *CSVConfig) *CSVNode {
	return &CSVNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute converts the configured property. Strings are parsed and the
// rows stashed for Expand; slices are serialized to a CSV string and
// written back to the property.
func (n *CSVNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.rows = nil
	n.header = nil

	prop := n.property()
	value, onData := nodebase.JSONSource(input, prop)

	switch v := value.(type) {
	case string:
		r := csv.NewReader(bytes.NewBufferString(v))
		r.FieldsPerRecord = -1 // tolerate ragged rows
		rows, err := r.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("csv parse failed: %w", err)
		}
		if n.config.Headers && len(rows) > 0 {
			n.header = rows[0]
			rows = rows[1:]
		}
		n.rows = rows
		return input, nil // Expand emits one message per row
	case []interface{}:
		s, err := serializeCSV(v, n.config.Headers)
		if err != nil {
			return nil, fmt.Errorf("csv serialize failed: %w", err)
		}
		if onData {
			input.SetPayload(s)
		} else {
			m, ok := input.GetPayload().(map[string]interface{})
			if !ok {
				m = map[string]interface{}{}
				input.SetPayload(m)
			}
			nodebase.SetPath(m, prop, s)
		}
		return input, nil
	default:
		return nil, fmt.Errorf("csv: property is %T, expected string or array", value)
	}
}

// Expand emits one message per parsed row. With headers each row is a
// map keyed by the header line; otherwise a []string. Every message
// carries parts metadata (id, type "csv", index, count) so a downstream
// join node can reassemble the table.
func (n *CSVNode) Expand(output nodestore.MessageInterface) []nodestore.MessageInterface {
	if n.rows == nil {
		return []nodestore.MessageInterface{output} // serialize mode: single output
	}
	if len(n.rows) == 0 {
		return nil // empty table: nothing to emit
	}

	prop := n.property()
	groupID := fmt.Sprintf("parts-csv-%d", nodebase.NextGroupSeq())
	count := len(n.rows)

	out := make([]nodestore.MessageInterface, 0, count)
	for i, row := range n.rows {
		p := nodebase.ClonePayload(output)
		if p.GetMetadata() == nil {
			p.SetMetadata(map[string]interface{}{})
		}

		var rowData interface{} = row
		if n.header != nil {
			m := make(map[string]interface{}, len(n.header))
			for j, col := range n.header {
				if j < len(row) {
					m[col] = row[j]
				}
			}
			rowData = m
		}

		if prop == "payload" {
			p.SetPayload(rowData)
		} else if m, ok := p.GetPayload().(map[string]interface{}); ok {
			nodebase.SetPath(m, prop, rowData)
		}

		p.GetMetadata()["parts"] = map[string]interface{}{
			"id":    groupID,
			"type":  "csv",
			"index": i,
			"count": count,
		}
		out = append(out, p)
	}
	return out
}

// property returns the configured source property (default "payload")
func (n *CSVNode) property() string {
	if n.config.Property == "" {
		return "payload"
	}
	return n.config.Property
}

// serializeCSV writes rows to a CSV string. Rows may be slices of
// scalars or maps; with headers=true a header row derived from the
// first map's sorted keys is emitted first.
func serializeCSV(rows []interface{}, headers bool) (string, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	var headerKeys []string
	if headers && len(rows) > 0 {
		if m, ok := rows[0].(map[string]interface{}); ok {
			headerKeys = nodebase.SortedKeys(m)
			if err := w.Write(headerKeys); err != nil {
				return "", err
			}
		}
	}

	for _, row := range rows {
		var record []string
		switch r := row.(type) {
		case map[string]interface{}:
			keys := headerKeys
			if keys == nil {
				keys = nodebase.SortedKeys(r)
			}
			for _, k := range keys {
				record = append(record, fmt.Sprintf("%v", r[k]))
			}
		case []interface{}:
			for _, cell := range r {
				record = append(record, fmt.Sprintf("%v", cell))
			}
		case []string:
			record = r
		default:
			return "", fmt.Errorf("unsupported row type %T", row)
		}
		if err := w.Write(record); err != nil {
			return "", err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return "", err
	}
	return buf.String(), nil
}
