// Package xml contains the xml node implementation.
package xml

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/xml"
	"fmt"
	"sort"
	"strconv"
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
		"attr": map[string]interface{}{
			"type":        "string",
			"title":       "Attribute Key",
			"description": "Object key element attributes are collected under (default \"$\"); an empty value merges attributes into the element object",
			"default":     "$",
		},
		"chr": map[string]interface{}{
			"type":        "string",
			"title":       "Character Key",
			"description": "Object key element text content is stored under (default \"_\")",
			"default":     "_",
		},
		"pretty": map[string]interface{}{
			"type":        "boolean",
			"title":       "Pretty Print",
			"description": "Indent marshalled output with two spaces",
			"default":     false,
		},
	},
}

// XMLConfig holds configuration for the xml node — a Node-RED XML
// parser equivalent producing xml2js-shaped objects.
type XMLConfig struct {
	// Property is the message field to convert; "payload" (default)
	// targets msg.Payload, otherwise a dotted path inside the data map
	Property string `json:"property"`
	// Attr is the object key element attributes are collected under
	// (default "$"); an empty value merges attributes into the element
	// object
	Attr string `json:"attr"`
	// Chr is the object key element text is stored under (default "_")
	Chr string `json:"chr"`
	// Pretty indents marshalled output with two spaces
	Pretty bool `json:"pretty"`
}

// xmlOptions carries the conversion options — resolved from the node
// config and overridable per message via msg.options.
type xmlOptions struct {
	explicitArray bool   // children always collected in arrays (default true)
	attrkey       string // key attributes are collected under (default "$")
	charkey       string // key text content is stored under (default "_")
	mergeAttrs    bool   // merge attributes into the element object
	trim          bool   // trim surrounding whitespace from text
	normalize     bool   // trim + collapse inner whitespace
	ignoreAttrs   bool   // drop attributes entirely
	rootName      string // wrapper element for marshalled objects (default "root")
}

func defaultXMLOptions(c *XMLConfig) xmlOptions {
	o := xmlOptions{
		explicitArray: true,
		attrkey:       "$",
		charkey:       "_",
		rootName:      "root",
	}
	if c != nil {
		if c.Attr != "" {
			o.attrkey = c.Attr
		}
		if c.Chr != "" {
			o.charkey = c.Chr
		}
	}
	return o
}

// applyMsgOptions merges a msg.options map into o — booleans are
// applied when present, so a flow can e.g. set explicitArray:false.
func (o *xmlOptions) applyMsgOptions(opts map[string]interface{}) {
	if opts == nil {
		return
	}
	if v, ok := opts["explicitArray"].(bool); ok {
		o.explicitArray = v
	}
	if v, ok := opts["mergeAttrs"].(bool); ok {
		o.mergeAttrs = v
	}
	if v, ok := opts["ignoreAttrs"].(bool); ok {
		o.ignoreAttrs = v
	}
	if v, ok := opts["trim"].(bool); ok {
		o.trim = v
	}
	if v, ok := opts["normalize"].(bool); ok {
		o.normalize = v
	}
	if v, ok := opts["attrkey"].(string); ok && v != "" {
		o.attrkey = v
	}
	if v, ok := opts["charkey"].(string); ok && v != "" {
		o.charkey = v
	}
	if v, ok := opts["rootName"].(string); ok && v != "" {
		o.rootName = v
	}
}

// XMLNode converts between an XML string and a generic map form using
// xml2js conventions:
//
//	Parse:   {tag: {"$": {attr: value}, childTag: [values], "_": text}}
//	Marshal: an object is wrapped in a root element and prefixed with
//	         the standard XML declaration.
//
// Per-message conversion options can be supplied via msg.options
// (explicitArray, mergeAttrs, ignoreAttrs, trim, normalize, attrkey,
// charkey, rootName). Element order is not preserved — payloads are
// unordered Go maps, so marshalled children emit in sorted order.
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

	opts := defaultXMLOptions(n.config)
	if md := input.GetMetadata(); md != nil {
		if msgOpts, ok := md["options"].(map[string]interface{}); ok {
			opts.applyMsgOptions(msgOpts)
		}
	}

	var result interface{}
	if s, ok := value.(string); ok {
		m, err := decodeXMLString(s, &opts)
		if err != nil {
			return nil, fmt.Errorf("xml parse failed: %w", err)
		}
		result = m
	} else {
		s, err := n.marshalXML(value, &opts)
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
// {rootTag: element} via a token walk.
func decodeXMLString(s string, o *xmlOptions) (map[string]interface{}, error) {
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
			elem, err := decodeXMLElement(dec, se, o)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{rootTag: elem}, nil
		}
	}
}

// decodeXMLElement consumes tokens until the matching end element and
// returns the element body. A text-only element with no attributes or
// children collapses to its text; otherwise the body is a map with the
// attribute key, grouped children and the char key for mixed text.
func decodeXMLElement(dec *xml.Decoder, start xml.StartElement, o *xmlOptions) (interface{}, error) {
	body := map[string]interface{}{}
	if o.mergeAttrs {
		for _, attr := range start.Attr {
			body[attr.Name.Local] = attr.Value
		}
	} else if !o.ignoreAttrs && len(start.Attr) > 0 {
		attrs := map[string]interface{}{}
		for _, attr := range start.Attr {
			attrs[attr.Name.Local] = attr.Value
		}
		body[o.attrkey] = attrs
	}

	var text strings.Builder
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("unexpected end of document inside <%s>", start.Name.Local)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			child, err := decodeXMLElement(dec, t, o)
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
			} else if o.explicitArray {
				body[name] = []interface{}{child}
			} else {
				body[name] = child
			}
		case xml.CharData:
			text.Write([]byte(t))
		case xml.EndElement:
			if t.Name.Local == start.Name.Local {
				txt := o.processText(text.String())
				if len(body) == 0 {
					return txt, nil // text-only element
				}
				if strings.TrimSpace(txt) != "" {
					body[o.charkey] = txt
				}
				return body, nil
			}
		}
	}
}

// processText applies the trim/normalize options to element text.
func (o *xmlOptions) processText(s string) string {
	if o.normalize {
		return strings.Join(strings.Fields(s), " ")
	}
	if o.trim {
		return strings.TrimSpace(s)
	}
	return s
}

// marshalXML converts a value into an XML document string. The object
// is wrapped in a root element (rootName option, default "root") and
// prefixed with the standard XML declaration — matching the xml2js
// builder output shape.
func (n *XMLNode) marshalXML(value interface{}, o *xmlOptions) (string, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("expected a map payload, got %T", value)
	}

	var buf bytes.Buffer
	buf.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	enc := xml.NewEncoder(&buf)
	if n.config.Pretty {
		enc.Indent("", "  ")
	}
	if err := encodeXMLElement(enc, o.rootName, m, o); err != nil {
		return "", err
	}
	if err := enc.Flush(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// encodeXMLElement writes one element: the attrkey map becomes
// attributes, charkey becomes character data, slices produce repeated
// elements and scalars become a text element.
func encodeXMLElement(enc *xml.Encoder, tag string, body map[string]interface{}, o *xmlOptions) error {
	var attrs []xml.Attr
	var children []string
	text := ""
	for _, k := range sortedXMLKeys(body, o) {
		v := body[k]
		if k == o.attrkey {
			if am, ok := v.(map[string]interface{}); ok {
				for _, ak := range sortedMapKeys(am) {
					attrs = append(attrs, xml.Attr{
						Name:  xml.Name{Local: ak},
						Value: scalarString(am[ak]),
					})
				}
			}
			continue
		}
		if k == o.charkey {
			text = scalarString(v)
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
				if err := encodeXMLValue(enc, k, item, o); err != nil {
					return err
				}
			}
		} else if err := encodeXMLValue(enc, k, v, o); err != nil {
			return err
		}
	}
	return enc.EncodeToken(start.End())
}

// encodeXMLValue writes a single element value (map body or scalar text)
func encodeXMLValue(enc *xml.Encoder, tag string, v interface{}, o *xmlOptions) error {
	if m, ok := v.(map[string]interface{}); ok {
		return encodeXMLElement(enc, tag, m, o)
	}
	return encodeXMLElement(enc, tag, map[string]interface{}{o.charkey: v}, o)
}

// scalarString renders a scalar the way JavaScript string coercion
// does: numbers drop a trailing ".0".
func scalarString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// sortedXMLKeys returns map keys in deterministic order: the attrkey
// and charkey first, then children sorted alphabetically (payloads are
// unordered Go maps, so document order cannot be recovered).
func sortedXMLKeys(m map[string]interface{}, o *xmlOptions) []string {
	var head, rest []string
	for k := range m {
		if k == o.attrkey || k == o.charkey {
			head = append(head, k)
		} else {
			rest = append(rest, k)
		}
	}
	sort.Strings(head)
	sort.Strings(rest)
	return append(head, rest...)
}

// sortedMapKeys returns a map's keys sorted alphabetically.
func sortedMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
