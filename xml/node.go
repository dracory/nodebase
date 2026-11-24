// Package xml contains the xml node implementation.
package xml

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/xml"
	"fmt"
	"sort"
	"strings"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(XMLConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewXMLNode(n, c), nil
}

// init registers the xml node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "xml",
			Name:        "XML",
			Description: "Converts between an XML string and a generic map",
			Icon:        "file-code",
			Category:    "Parsers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the xml node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Field to convert; \"payload\" (default) or a dotted path inside the payload",
			"default":     "payload",
		},
		"pretty": map[string]interface{}{
			"type":        "boolean",
			"title":       "Pretty Print",
			"description": "Indent marshalled output with two spaces",
			"default":     false,
		},
	},
}

// XMLConfig holds configuration for the xml node — a simplified
// Node-RED XML parser equivalent.
type XMLConfig struct {
	// Property is the message field to convert; "payload" (default)
	// targets msg.Payload, otherwise a dotted path inside the data map
	Property string `json:"property"`
	// Pretty indents marshalled output with two spaces
	Pretty bool `json:"pretty"`
}

// XMLNode converts between an XML string and a generic map form.
//
// Map shape produced by parsing:
//
//	{tag: {"@attr": value, childTag: child | []child, "#text": "..."}}
//
// The marshal direction accepts that same shape plus flat maps of
// {tag: scalar}; a map with several keys is wrapped in a <root>
// element. encoding/xml is lossy compared to Node-RED's xml2js —
// element order, mixed content and namespaces are not preserved.
type XMLNode struct {
	*nodebase.BaseNode
	config *XMLConfig
}

// NewXMLNode creates a new xml node
func NewXMLNode(node nodestore.NodeInterface, config *XMLConfig) *XMLNode {
	return &XMLNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute reads the configured property, converts it and writes the
// result back: string → map (parse), map/other → XML string (marshal).
func (n *XMLNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	prop := n.property()
	value, onData := nodebase.JSONSource(input, prop)

	var result interface{}
	if s, ok := value.(string); ok {
		m, err := decodeXMLString(s)
		if err != nil {
			return nil, fmt.Errorf("xml parse failed: %w", err)
		}
		result = m
	} else {
		s, err := n.marshalXML(value)
		if err != nil {
			return nil, fmt.Errorf("xml marshal failed: %w", err)
		}
		result = s
	}

	if onData {
		input.SetPayload(result)
	} else {
		m, ok := input.GetPayload().(map[string]interface{})
		if !ok {
			m = map[string]interface{}{}
			input.SetPayload(m)
		}
		nodebase.SetPath(m, prop, result)
	}
	return input, nil
}

// property returns the configured source property (default "payload")
func (n *XMLNode) property() string {
	if n.config.Property == "" {
		return "payload"
	}
	return n.config.Property
}

// decodeXMLString parses an XML document into the generic map form
// {rootTag: node} via a token walk.
func decodeXMLString(s string) (map[string]interface{}, error) {
	dec := xml.NewDecoder(strings.NewReader(s))

	// Find the root start element
	var rootTag string
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("no root element found")
		}
		if se, ok := tok.(xml.StartElement); ok {
			rootTag = se.Name.Local
			elem, err := decodeXMLElement(dec, se)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{rootTag: elem}, nil
		}
	}
}

// decodeXMLElement consumes tokens until the matching end element and
// returns the element body as a map (or a bare string for text-only
// elements with no attributes/children).
func decodeXMLElement(dec *xml.Decoder, start xml.StartElement) (interface{}, error) {
	body := map[string]interface{}{}
	for _, attr := range start.Attr {
		body["@"+attr.Name.Local] = attr.Value
	}

	var text strings.Builder
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("unexpected end of document inside <%s>", start.Name.Local)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			child, err := decodeXMLElement(dec, t)
			if err != nil {
				return nil, err
			}
			name := t.Name.Local
			if prev, exists := body[name]; exists {
				if list, ok := prev.([]interface{}); ok {
					body[name] = append(list, child)
				} else {
					body[name] = []interface{}{prev, child}
				}
			} else {
				body[name] = child
			}
		case xml.CharData:
			text.Write([]byte(t))
		case xml.EndElement:
			if t.Name.Local == start.Name.Local {
				txt := strings.TrimSpace(text.String())
				if len(body) == 0 {
					return txt, nil // text-only element
				}
				if txt != "" {
					body["#text"] = txt
				}
				return body, nil
			}
		}
	}
}

// marshalXML converts a value into an XML document string
func (n *XMLNode) marshalXML(value interface{}) (string, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("expected a map payload, got %T", value)
	}

	// Single-key map: the key is the root tag. Several keys (or none)
	// are wrapped in a synthetic <root> element.
	rootTag := "root"
	children := m
	if len(m) == 1 {
		for k, v := range m {
			rootTag = k
			if inner, ok := v.(map[string]interface{}); ok {
				children = inner
			} else {
				children = map[string]interface{}{"#text": v}
			}
		}
	}

	var buf bytes.Buffer
	enc := xml.NewEncoder(&buf)
	if n.config.Pretty {
		enc.Indent("", "  ")
	}
	if err := encodeXMLElement(enc, rootTag, children); err != nil {
		return "", err
	}
	if err := enc.Flush(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// encodeXMLElement writes one element: "@x" keys become attributes,
// "#text" becomes character data, slices produce repeated elements and
// scalars become a text element.
func encodeXMLElement(enc *xml.Encoder, tag string, body map[string]interface{}) error {
	var attrs []xml.Attr
	var children []string
	text := ""
	for _, k := range sortedXMLKeys(body) {
		v := body[k]
		if strings.HasPrefix(k, "@") {
			attrs = append(attrs, xml.Attr{
				Name:  xml.Name{Local: k[1:]},
				Value: fmt.Sprintf("%v", v),
			})
			continue
		}
		if k == "#text" {
			text = fmt.Sprintf("%v", v)
			continue
		}
		children = append(children, k)
	}

	start := xml.StartElement{Name: xml.Name{Local: tag}, Attr: attrs}
	if err := enc.EncodeToken(start); err != nil {
		return err
	}
	if text != "" {
		if err := enc.EncodeToken(xml.CharData(text)); err != nil {
			return err
		}
	}
	for _, k := range children {
		v := body[k]
		if list, ok := v.([]interface{}); ok {
			for _, item := range list {
				if err := encodeXMLValue(enc, k, item); err != nil {
					return err
				}
			}
		} else if err := encodeXMLValue(enc, k, v); err != nil {
			return err
		}
	}
	return enc.EncodeToken(start.End())
}

// encodeXMLValue writes a single element value (map body or scalar text)
func encodeXMLValue(enc *xml.Encoder, tag string, v interface{}) error {
	if m, ok := v.(map[string]interface{}); ok {
		return encodeXMLElement(enc, tag, m)
	}
	return encodeXMLElement(enc, tag, map[string]interface{}{"#text": v})
}

// sortedXMLKeys returns map keys in deterministic order: attributes
// first, then children, "#text" position is irrelevant.
func sortedXMLKeys(m map[string]interface{}) []string {
	var attrs, text, rest []string
	for k := range m {
		switch {
		case strings.HasPrefix(k, "@"):
			attrs = append(attrs, k)
		case k == "#text":
			text = append(text, k)
		default:
			rest = append(rest, k)
		}
	}
	sort.Strings(attrs)
	sort.Strings(rest)
	return append(append(attrs, text...), rest...)
}
