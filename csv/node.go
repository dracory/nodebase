// Package csv contains the csv node implementation.
package csv

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(CSVConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewCSVNode(n, c, e), nil
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
	// HdrIn is Node-RED's header flag (serialized "hdrin"); same effect
	// as Headers
	HdrIn bool `json:"hdrin"`
	// HdrOut controls header emission when serializing (serialized
	// "hdrout"): "none" (default) never emits a header row, "all" emits
	// one on every output, "once" emits one until a message carrying a
	// reset property arrives
	HdrOut string `json:"hdrout"`
	// Temp is the column-name template (serialized "temp"): a
	// separator-delimited list of column names used to key parsed rows
	// and to select/order columns when serializing objects
	Temp string `json:"temp"`
	// Multi is Node-RED's output mode (serialized "multi"): "one"
	// emits one message per row as a parts sequence; "mult" produces a
	// single message whose payload is the whole parsed table. Unset
	// keeps the nodebase expansion behavior (one message per row).
	Multi string `json:"multi"`
	// Sep is the column separator (serialized "sep"); default ","
	Sep string `json:"sep"`
	// Ret is the row terminator hint (serialized "ret", e.g. "\n");
	// informational only — the parser accepts both endings
	Ret string `json:"ret"`
	// Skip skips this many leading lines before parsing (serialized "skip")
	Skip int `json:"skip"`
	// Strings is Node-RED's "strings" flag: true (and unset) coerces
	// numeric-looking cells to numbers via parseFloat
	Strings *bool `json:"strings"`
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
	// msgCols holds column names supplied by the triggering message
	// (msg.columns) — used when no template is configured
	msgCols []string
	// host and workflowID locate the per-run state slot — executors
	// are rebuilt per message, so hdrout "once" state lives there
	host       nodebase.NodeHost
	workflowID string
	// local is the fallback state for host-less constructions (tests)
	local *csvState
}

// csvState is the per-run mutable state for the csv node.
type csvState struct {
	mu sync.Mutex
	// hdrSent records that a "once" header row was already written;
	// a message carrying a reset property clears it
	hdrSent bool
}

// NewCSVNode creates a new csv node
func NewCSVNode(node nodestore.NodeInterface, config *CSVConfig, host nodebase.NodeHost) *CSVNode {
	return &CSVNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// state returns (creating if needed) the per-run csv state — host-backed
// when a host is wired, local otherwise.
func (n *CSVNode) state() *csvState {
	if n.host != nil {
		return n.host.GetOrCreateNodeState(n.workflowID, n.GetID(), func() interface{} {
			return &csvState{}
		}).(*csvState)
	}
	if n.local == nil {
		n.local = &csvState{}
	}
	return n.local
}

// Execute converts the configured property. Strings are parsed and the
// rows stashed for Expand; slices are serialized to a CSV string and
// written back to the property.
func (n *CSVNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.rows = nil
	n.header = nil
	n.msgCols = messageColumns(input, n.sep())

	// A reset property re-arms the "once" header row (and would drop
	// any learned column state — header rows are re-read per input).
	if _, ok := input.GetMetadata()["reset"]; ok {
		st := n.state()
		st.mu.Lock()
		st.hdrSent = false
		st.mu.Unlock()
	}

	prop := n.property()
	value, onData := nodebase.JSONSource(input, prop)

	switch v := value.(type) {
	case string:
		if n.config.Skip > 0 {
			v = skipLines(v, n.config.Skip)
		}
		r := csv.NewReader(bytes.NewBufferString(v))
		if n.config.Sep != "" {
			if rr := []rune(n.config.Sep); len(rr) > 0 {
				r.Comma = rr[0]
			}
		}
		r.FieldsPerRecord = -1 // tolerate ragged rows
		rows, err := r.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("csv parse failed: %w", err)
		}
		if n.headers() && len(rows) > 0 {
			n.header = rows[0]
			rows = rows[1:]
		}
		if n.config.Multi == "mult" {
			// Single-message mode: the whole table as an array
			// of row objects, plus a "columns" metadata list
			// like Node-RED.
			n.writeValue(input, prop, onData, n.tableValue(rows))
			if cols := n.columns(); cols != "" {
				if input.GetMetadata() == nil {
					input.SetMetadata(map[string]interface{}{})
				}
				input.GetMetadata()["columns"] = cols
			}
			return input, nil
		}
		n.rows = rows
		return input, nil // Expand emits one message per row
	case []interface{}:
		s, err := n.serializeRows(v)
		if err != nil {
			return nil, fmt.Errorf("csv serialize failed: %w", err)
		}
		n.writeValue(input, prop, onData, s)
		return input, nil
	case map[string]interface{}:
		// A single object serializes as one row (column selection
		// and header rules same as an array of objects).
		s, err := n.serializeRows([]interface{}{v})
		if err != nil {
			return nil, fmt.Errorf("csv serialize failed: %w", err)
		}
		n.writeValue(input, prop, onData, s)
		return input, nil
	default:
		return nil, fmt.Errorf("csv: property is %T, expected string or array", value)
	}
}

// Expand emits one message per parsed row. Each row is an object keyed
// by the resolved column names — the header row, the configured
// template, message-provided columns, or generated col1..colN. Every
// message carries parts metadata (id, type "csv", index, count) so a
// downstream join node can reassemble the table.
func (n *CSVNode) Expand(output nodestore.MessageInterface) []nodestore.MessageInterface {
	if n.rows == nil {
		return []nodestore.MessageInterface{output} // serialize mode: single output
	}
	if len(n.rows) == 0 {
		return nil // empty table: nothing to emit
	}

	prop := n.property()
	// Node-RED keys the sequence id on the triggering message's id.
	groupID := output.GetID()
	if groupID == "" {
		groupID = fmt.Sprintf("parts-csv-%d", nodebase.NextGroupSeq())
	}
	count := len(n.rows)
	cols := n.columns()

	out := make([]nodestore.MessageInterface, 0, count)
	for i, row := range n.rows {
		p := nodebase.ClonePayload(output)
		if p.GetMetadata() == nil {
			p.SetMetadata(map[string]interface{}{})
		}

		rowData := rowObject(n.rowColumns(len(row)), row, n.parseStrings())

		if prop == "payload" {
			p.SetPayload(rowData)
		} else if m, ok := p.GetPayload().(map[string]interface{}); ok {
			nodebase.SetPath(m, prop, rowData)
		}

		if cols != "" {
			p.GetMetadata()["columns"] = cols
		}
		p.GetMetadata()["parts"] = map[string]interface{}{
			"id":    groupID,
			"type":  "csv",
			"index": i,
			"count": count,
		}
		if i == count-1 {
			p.GetMetadata()["complete"] = true
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

// headers reports whether the first row is a header line
func (n *CSVNode) headers() bool {
	return n.config.Headers || n.config.HdrIn
}

// parseStrings reports whether numeric-looking cells coerce to
// numbers — Node-RED's "strings" flag defaults to true when unset.
func (n *CSVNode) parseStrings() bool {
	return n.config.Strings == nil || *n.config.Strings
}

// sep returns the configured column separator (default ",")
func (n *CSVNode) sep() string {
	if n.config.Sep == "" {
		return ","
	}
	return n.config.Sep
}

// rowColumns resolves the column names for a parsed row: the header
// row when set, else message-provided columns or the configured
// template, else generated col1..colN names covering the row.
func (n *CSVNode) rowColumns(rowLen int) []string {
	if n.header != nil {
		return n.header
	}
	if c := n.namedColumns(); c != nil {
		return c
	}
	cols := make([]string, rowLen)
	for i := range cols {
		cols[i] = fmt.Sprintf("col%d", i+1)
	}
	return cols
}

// namedColumns returns the configured column template, or the column
// names supplied by the triggering message (msg.columns) when no
// template is configured; nil when neither is set.
func (n *CSVNode) namedColumns() []string {
	if n.config.Temp != "" {
		return splitColumns(n.config.Temp, n.sep())
	}
	if n.msgCols != nil {
		return n.msgCols
	}
	return nil
}

// columns returns the "columns" metadata Node-RED attaches: the
// resolved column names joined by commas (empty when columns are
// auto-generated).
func (n *CSVNode) columns() string {
	cols := n.header
	if cols == nil {
		cols = n.namedColumns()
	}
	if cols == nil {
		return ""
	}
	return strings.Join(cols, ",")
}

// splitColumns splits a column-name list on the separator, trimming
// whitespace around each name.
func splitColumns(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// messageColumns reads msg.columns (a separator-delimited string or an
// array of strings) off the message metadata; nil when absent.
func messageColumns(input nodestore.MessageInterface, sep string) []string {
	if input == nil || input.GetMetadata() == nil {
		return nil
	}
	switch c := input.GetMetadata()["columns"].(type) {
	case string:
		return splitColumns(c, sep)
	case []interface{}:
		out := make([]string, 0, len(c))
		for _, v := range c {
			out = append(out, fmt.Sprintf("%v", v))
		}
		return out
	case []string:
		return c
	}
	return nil
}

// tableValue renders parsed rows as the single-message payload: an
// array of row objects keyed by the resolved column names.
func (n *CSVNode) tableValue(rows [][]string) []interface{} {
	out := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		out = append(out, rowObject(n.rowColumns(len(row)), row, n.parseStrings()))
	}
	return out
}

// rowObject maps a CSV row onto the header columns
func rowObject(header, row []string, coerce bool) map[string]interface{} {
	m := make(map[string]interface{}, len(header))
	for j, col := range header {
		if j < len(row) {
			m[col] = cellValue(row[j], coerce)
		}
	}
	return m
}

// numericCell mirrors Node-RED's coercion regex: numbers parse, but a
// leading zero before a digit ("0123") keeps the string — parseFloat
// would misread it.
var numericCell = regexp.MustCompile(`^-?((0)|([1-9]\d*))(\.\d+)?([eE][-+]?\d+)?$`)

// cellValue coerces a numeric-looking cell to float64 when coerce is
// set (Node-RED parseFloat behavior).
func cellValue(s string, coerce bool) interface{} {
	if coerce {
		trimmed := strings.TrimSpace(s)
		if numericCell.MatchString(trimmed) {
			if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
				return f
			}
		}
	}
	return s
}

// writeValue writes v into prop on the message
func (n *CSVNode) writeValue(input nodestore.MessageInterface, prop string, onData bool, v interface{}) {
	if onData {
		input.SetPayload(v)
		return
	}
	m, ok := input.GetPayload().(map[string]interface{})
	if !ok {
		m = map[string]interface{}{}
		input.SetPayload(m)
	}
	nodebase.SetPath(m, prop, v)
}

// skipLines drops the first n lines of s (Node-RED's skip option —
// used for pre-header preamble lines).
func skipLines(s string, n int) string {
	for i := 0; i < n && len(s) > 0; i++ {
		idx := -1
		for j, c := range s {
			if c == '\n' {
				idx = j
				break
			}
		}
		if idx < 0 {
			return ""
		}
		s = s[idx+1:]
	}
	return s
}

// serializeRows writes rows to a CSV string. Rows may be maps or
// slices; column names come from msg.columns or the configured
// template (selecting and ordering object keys), else from each map's
// sorted keys. hdrout "all" prefixes every output with the column
// list, "once" only until a message carrying reset arrives.
func (n *CSVNode) serializeRows(rows []interface{}) (string, error) {
	st := n.state()
	st.mu.Lock()
	defer st.mu.Unlock()

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if n.config.Sep != "" {
		if rr := []rune(n.config.Sep); len(rr) > 0 {
			w.Comma = rr[0]
		}
	}

	cols := n.namedColumns()
	if cols == nil {
		for _, row := range rows {
			if m, ok := row.(map[string]interface{}); ok {
				cols = nodebase.SortedKeys(m)
				break
			}
		}
	}

	if cols != nil && len(rows) > 0 && n.writeHeader(st) {
		if err := w.Write(cols); err != nil {
			return "", err
		}
		st.hdrSent = true
	}

	for _, row := range rows {
		var record []string
		switch r := row.(type) {
		case map[string]interface{}:
			keys := cols
			if keys == nil {
				keys = nodebase.SortedKeys(r)
			}
			for _, k := range keys {
				v, ok := r[k]
				if !ok || v == nil {
					record = append(record, "")
				} else {
					record = append(record, fmt.Sprintf("%v", v))
				}
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

// writeHeader reports whether this output should carry a column-name
// header row per the hdrout setting; the caller holds st.mu.
func (n *CSVNode) writeHeader(st *csvState) bool {
	switch n.config.HdrOut {
	case "all":
		return true
	case "once":
		return !st.hdrSent
	}
	return false
}
