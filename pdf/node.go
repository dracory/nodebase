// Package pdf contains the pdf node: it renders the incoming payload
// to a PDF document (invoices, reports). Two modes: "text" (payload
// string becomes a paginated text PDF) and "template" (payload
// {title, fields, table, body} drives a report layout).
package pdf

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	gofpdf "github.com/jung-kurt/gofpdf/v2"

	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &PDFConfig{Mode: "text", Filename: "document.pdf"}
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	if c.Filename == "" {
		c.Filename = "document.pdf"
	}
	if !strings.HasSuffix(strings.ToLower(c.Filename), ".pdf") {
		c.Filename += ".pdf"
	}
	return NewPDFNode(n, c), nil
}

// init registers the pdf node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "pdf",
			Name:        "PDF",
			Description: "Renders the payload to a PDF document (invoices, reports)",
			Icon:        "file-pdf",
			Category:    "Document",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the pdf node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"mode": map[string]interface{}{
			"type":        "string",
			"title":       "Mode",
			"enum":        []string{"text", "template"},
			"default":     "text",
			"description": "text: payload string becomes a paginated text PDF; template: payload {title, fields, table, body} drives a report layout",
		},
		"filename": map[string]string{
			"type":        "string",
			"title":       "Filename",
			"description": "Suggested file name stamped into msg.filename / msg.file.suggestedName",
		},
		"title": map[string]string{
			"type":        "string",
			"title":       "Title",
			"description": "Template mode heading; a payload \"title\" key wins when present",
		},
	},
}

// PDFConfig holds the `pdf` node configuration.
type PDFConfig struct {
	// Mode: "text" (payload string -> paginated text PDF) or
	// "template" (payload drives a title/fields/table layout).
	Mode     string `json:"mode"`
	Filename string `json:"filename"`
	// Title is the heading for template mode; a payload "title" key
	// wins when present.
	Title string `json:"title"`
}

// PDFNode renders the incoming payload to a PDF document. Output:
// msg.Payload = PDF bytes; Metadata["file"] carries
// {suggestedName, contentType:"application/pdf", size} and
// Metadata["filename"] the suggested name so a `file` node with
// filenameType "msg" writes it directly.
type PDFNode struct {
	*nodebase.BaseNode
	cfg *PDFConfig
}

// NewPDFNode creates a new pdf node.
func NewPDFNode(node nodestore.NodeInterface, cfg *PDFConfig) *PDFNode {
	return &PDFNode{BaseNode: nodebase.NewBaseNode(node), cfg: cfg}
}

// Execute renders the payload to PDF and stamps the file metadata.
func (n *PDFNode) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	doc, err := n.render(in.GetPayload())
	if err != nil {
		return nil, err
	}
	in.SetPayload(doc)
	metadata := in.GetMetadata()
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	metadata["file"] = map[string]interface{}{
		"suggestedName": n.cfg.Filename,
		"contentType":   "application/pdf",
		"size":          len(doc),
	}
	metadata["filename"] = n.cfg.Filename
	in.SetMetadata(metadata)
	return in, nil
}

func (n *PDFNode) render(data interface{}) ([]byte, error) {
	doc := gofpdf.New("P", "mm", "A4", "")
	doc.SetCreator("GoFlow pdf node", true)
	doc.AddPage()
	doc.SetFont("Helvetica", "", 11)

	if n.cfg.Mode == "template" {
		if m, ok := data.(map[string]interface{}); ok {
			renderTemplate(doc, n.cfg.Title, m)
		} else {
			renderText(doc, payloadText(data))
		}
	} else {
		renderText(doc, payloadText(data))
	}

	var buf bytes.Buffer
	if err := doc.Output(&buf); err != nil {
		return nil, fmt.Errorf("pdf render failed: %w", err)
	}
	if buf.Len() == 0 {
		return nil, fmt.Errorf("pdf render produced no output")
	}
	return buf.Bytes(), nil
}

// payloadText coerces the payload to text: strings pass through,
// []byte is decoded, anything else is JSON-encoded.
func payloadText(data interface{}) string {
	switch v := data.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		raw, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(raw)
	}
}

// renderText writes a simple paginated text document.
func renderText(doc *gofpdf.Fpdf, text string) {
	for _, line := range strings.Split(text, "\n") {
		doc.MultiCell(0, 6, line, "", "", false)
	}
}

// renderTemplate lays out a map payload as title / fields / table —
// the honest v1 "report" layout (not arbitrary HTML).
func renderTemplate(doc *gofpdf.Fpdf, configTitle string, m map[string]interface{}) {
	title := configTitle
	if t, ok := m["title"].(string); ok && t != "" {
		title = t
	}
	if title != "" {
		doc.SetFont("Helvetica", "B", 18)
		doc.Cell(0, 12, title)
		doc.Ln(16)
		doc.SetFont("Helvetica", "", 11)
	}

	if fields, ok := m["fields"].(map[string]interface{}); ok {
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			doc.SetFont("Helvetica", "B", 11)
			doc.Cell(50, 7, k+":")
			doc.SetFont("Helvetica", "", 11)
			doc.MultiCell(0, 7, fmt.Sprintf("%v", fields[k]), "", "", false)
		}
		doc.Ln(4)
	}

	if table, ok := m["table"].([]interface{}); ok {
		doc.SetFont("Helvetica", "", 10)
		for _, row := range table {
			switch cells := row.(type) {
			case []interface{}:
				for _, c := range cells {
					doc.CellFormat(45, 7, fmt.Sprintf("%v", c), "1", 0, "", false, 0, "")
				}
				doc.Ln(7)
			default:
				doc.CellFormat(0, 7, fmt.Sprintf("%v", cells), "1", 0, "", false, 0, "")
			}
		}
		doc.Ln(4)
	}

	if body, ok := m["body"].(string); ok && body != "" {
		doc.SetFont("Helvetica", "", 11)
		doc.MultiCell(0, 6, body, "", "", false)
	}
}
