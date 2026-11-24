// Package doctotext contains the docToText node: it extracts text
// from a document payload (pdf/docx/odt/pptx/txt) — the inverse of
// the `pdf` node. Output: msg.Payload = extracted text;
// Metadata["doc"] = {sourceFormat, mime}.
package doctotext

import (
	"archive/zip"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"

	pdfreader "github.com/ledongthuc/pdf"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := &DocToTextConfig{Format: "auto"}
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewDocToTextNode(n, c), nil
}

// init registers the docToText node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "docToText",
			Name:        "Doc To Text",
			Description: "Extracts text from a document payload (pdf/docx/odt/pptx/txt)",
			Icon:        "file-alt",
			Category:    "Parsers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the docToText node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"format": map[string]interface{}{
			"type":        "string",
			"title":       "Format",
			"enum":        []string{"auto", "pdf", "docx", "odt", "pptx", "txt"},
			"default":     "auto",
			"description": "auto detects the format from the payload's magic bytes",
		},
	},
}

// DocToTextConfig holds the `docToText` node configuration.
type DocToTextConfig struct {
	// Format: "auto" (default — detect from magic bytes) or a forced
	// "pdf" | "docx" | "odt" | "pptx" | "txt".
	Format string `json:"format"`
}

// DocToTextNode extracts text from a document payload — the inverse
// of `pdf`.
type DocToTextNode struct {
	*nodebase.BaseNode
	cfg *DocToTextConfig
}

// NewDocToTextNode creates a new docToText node.
func NewDocToTextNode(node nodestore.NodeInterface, cfg *DocToTextConfig) *DocToTextNode {
	return &DocToTextNode{BaseNode: nodebase.NewBaseNode(node), cfg: cfg}
}

// docMimes maps detected source formats to their MIME type.
var docMimes = map[string]string{
	"pdf":  "application/pdf",
	"docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"odt":  "application/vnd.oasis.opendocument.text",
	"pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	"txt":  "text/plain",
}

// Execute extracts the text and stamps the doc metadata.
func (n *DocToTextNode) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	var raw []byte
	switch v := in.GetPayload().(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("docToText: payload is not a document: %w", err)
		}
		raw = b
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("docToText: empty payload")
	}

	format := n.cfg.Format
	if format == "" || format == "auto" {
		format = detectDocFormat(raw)
	}
	text, err := extractDocText(format, raw)
	if err != nil {
		return nil, err
	}

	in.SetPayload(text)
	metadata := in.GetMetadata()
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	metadata["doc"] = map[string]interface{}{
		"sourceFormat": format,
		"mime":         docMimes[format],
	}
	in.SetMetadata(metadata)
	return in, nil
}

// detectDocFormat sniffs the document type from magic bytes: "%PDF"
// for PDF, "PK" zip containers are opened and inspected for the
// member names that distinguish docx / odt / pptx; anything else is
// treated as plain text.
func detectDocFormat(raw []byte) string {
	if bytes.HasPrefix(raw, []byte("%PDF")) {
		return "pdf"
	}
	if bytes.HasPrefix(raw, []byte("PK")) {
		zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			return "txt"
		}
		names := map[string]bool{}
		for _, f := range zr.File {
			names[f.Name] = true
		}
		switch {
		case names["word/document.xml"]:
			return "docx"
		case names["content.xml"]:
			return "odt"
		default:
			for n := range names {
				if strings.HasPrefix(n, "ppt/slides/slide") {
					return "pptx"
				}
			}
			return "txt"
		}
	}
	return "txt"
}

// extractDocText pulls plain text out of the document. pdf goes
// through ledongthuc/pdf (pure Go); the Office formats are zip+XML
// and are parsed with the stdlib.
func extractDocText(format string, raw []byte) (string, error) {
	switch format {
	case "pdf":
		r, err := pdfreader.NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			return "", fmt.Errorf("docToText: pdf open failed: %w", err)
		}
		plain, err := r.GetPlainText()
		if err != nil {
			return "", fmt.Errorf("docToText: pdf extract failed: %w", err)
		}
		b, err := io.ReadAll(plain)
		if err != nil {
			return "", fmt.Errorf("docToText: pdf read failed: %w", err)
		}
		return string(b), nil
	case "docx", "odt", "pptx":
		zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			return "", fmt.Errorf("docToText: %s open failed: %w", format, err)
		}
		return extractOfficeText(format, zr)
	case "txt":
		return string(raw), nil
	default:
		return "", fmt.Errorf("docToText: unsupported format %q", format)
	}
}

// extractOfficeText collects character data from the text-bearing
// XML members of an Office document.
func extractOfficeText(format string, zr *zip.Reader) (string, error) {
	var members []string
	// textElems: element local names whose character data is text.
	// breakElems: element local names that end a line.
	var textElems, breakElems map[string]bool
	switch format {
	case "docx":
		members = []string{"word/document.xml"}
		textElems = map[string]bool{"t": true}
		breakElems = map[string]bool{"p": true, "br": true, "tr": true}
	case "odt":
		members = []string{"content.xml"}
		textElems = map[string]bool{"p": true, "h": true, "span": true, "s": true}
		breakElems = map[string]bool{"p": true, "h": true}
	case "pptx":
		for _, f := range zr.File {
			if strings.HasPrefix(f.Name, "ppt/slides/slide") && strings.HasSuffix(f.Name, ".xml") {
				members = append(members, f.Name)
			}
		}
		sort.Strings(members)
		textElems = map[string]bool{"t": true}
		breakElems = map[string]bool{"p": true}
	}

	var sb strings.Builder
	for _, name := range members {
		f := zipFile(zr, name)
		if f == nil {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return "", fmt.Errorf("docToText: read %s failed: %w", name, err)
		}
		if err := collectXMLText(rc, &sb, textElems, breakElems); err != nil {
			rc.Close()
			return "", fmt.Errorf("docToText: parse %s failed: %w", name, err)
		}
		rc.Close()
	}
	return strings.TrimSpace(sb.String()), nil
}

func zipFile(zr *zip.Reader, name string) *zip.File {
	for _, f := range zr.File {
		if f.Name == name {
			return f
		}
	}
	return nil
}

// collectXMLText streams an XML document, appending character data of
// text elements and a newline at every break element.
func collectXMLText(r io.Reader, sb *strings.Builder, textElems, breakElems map[string]bool) error {
	dec := xml.NewDecoder(r)
	inText := 0
	lineStart := true
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			local := t.Name.Local
			if textElems[local] {
				inText++
				if !lineStart && local == "s" {
					sb.WriteByte(' ')
				}
			}
		case xml.EndElement:
			local := t.Name.Local
			if textElems[local] && inText > 0 {
				inText--
			}
			if breakElems[local] {
				sb.WriteByte('\n')
				lineStart = true
			}
		case xml.CharData:
			if inText > 0 {
				sb.Write(t)
				lineStart = false
			}
		}
	}
}
