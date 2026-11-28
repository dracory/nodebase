// Package html contains the html node implementation.
package html

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	xhtml "golang.org/x/net/html"
	"strings"
	"sync/atomic"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(HTMLConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewHTMLNode(n, c), nil
}

// init registers the html node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "html",
			Name:        "HTML",
			Description: "Extracts elements/text/attributes from HTML via a CSS selector",
			Icon:        "code",
			Category:    "Parsers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the html node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"tag": map[string]interface{}{
			"type":  "string",
			"title": "Selector",
			"description": "CSS selector. Supported subset: tag, .class, #id, [attr], [attr=value], " +
				"descendant and > combinators, comma groups",
		},
		"ret": map[string]interface{}{
			"type":    "string",
			"title":   "Return",
			"default": "html",
			"description": "html: element outer HTML; text: element text; val: the value " +
				"attribute; any other value names an attribute (Node-RED semantics)",
		},
		"as": map[string]interface{}{
			"type":    "string",
			"title":   "Output",
			"enum":    []string{"single", "multi"},
			"default": "single",
			"description": "single: one message with an array payload; " +
				"multi: one parts-tagged message per match",
		},
		"property": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"description": "Message property holding the HTML",
			"default":     "payload",
		},
		"outproperty": map[string]interface{}{
			"type":        "string",
			"title":       "Output Property",
			"description": "Message property the extraction is written to (defaults to property)",
			"default":     "payload",
		},
	},
	"required": []string{"tag"},
}

// htmlGroupSeq generates unique group ids for html node parts metadata.
var htmlGroupSeq int64

// HTMLConfig holds configuration for the html node — the Node-RED
// `html` equivalent: it extracts elements from an HTML message payload
// via a CSS selector.
type HTMLConfig struct {
	// Tag is the CSS selector. Supported subset: tag, .class, #id,
	// [attr], [attr=value], descendant and child (>) combinators,
	// comma-separated groups.
	Tag string `json:"tag"`
	// Ret selects what each match produces: "html" (outer HTML,
	// default), "text" (element text), "val" (the value attribute),
	// or any other string naming an attribute (Node-RED semantics).
	Ret string `json:"ret"`
	// As selects the output shape: "single" (one message whose payload
	// is an array of matches — Node-RED default) or "multi" (one
	// parts-tagged message per match).
	As string `json:"as"`
	// Property is the message property holding the HTML
	// (default "payload").
	Property string `json:"property"`
	// OutProperty is the message property the extraction is written to
	// (defaults to Property).
	OutProperty string `json:"outproperty"`
}

// HTMLNode extracts elements/text/attributes from an HTML payload.
// "single" mode stashes matches for the MessageExpander hook so each
// match travels downstream as its own parts-tagged message.
type HTMLNode struct {
	*nodebase.BaseNode
	config *HTMLConfig
	// matches holds pending per-match output for "single" mode
	matches []interface{}
}

// NewHTMLNode creates a new html node
func NewHTMLNode(node nodestore.NodeInterface, config *HTMLConfig) *HTMLNode {
	return &HTMLNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute parses the payload HTML and evaluates the selector. "single"
// (the default) writes an array of matches to the output property;
// "multi" stashes matches and returns a shell expanded by Expand into
// one parts-tagged message per match.
func (n *HTMLNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	property := n.config.Property
	if property == "" {
		property = "payload"
	}
	src, ok := nodebase.LookupProperty(input, property)
	if !ok {
		return nil, fmt.Errorf("html node: message has no %q property", property)
	}
	var doc string
	switch v := src.(type) {
	case string:
		doc = v
	case []byte:
		doc = string(v)
	default:
		return nil, fmt.Errorf("html node: %q is not a string", property)
	}

	// The configured selector may be left blank, in which case the
	// incoming message supplies it via msg.select.
	selector := n.config.Tag
	if selector == "" {
		if md := input.GetMetadata(); md != nil {
			selector, _ = md["select"].(string)
		}
	}
	if selector == "" {
		return nil, fmt.Errorf("html node: no selector (set tag or msg.select)")
	}
	sel, err := parseCSSSelector(selector)
	if err != nil {
		return nil, fmt.Errorf("html node: %w", err)
	}

	root, err := xhtml.Parse(strings.NewReader(doc))
	if err != nil {
		return nil, fmt.Errorf("html node parse failed: %w", err)
	}
	matches := findAll(root, sel)

	n.matches = n.matches[:0]
	for _, m := range matches {
		n.matches = append(n.matches, n.extract(m))
	}

	metadata := nodebase.CloneMetadata(input)
	out := nodestore.NewMessage().SetMetadata(metadata).SetTimestamp(input.GetTimestamp())
	if n.config.As == "multi" {
		return out, nil // expanded per match by Expand
	}
	nodebase.NodeSetProperty(out, n.outProperty(), n.matches)
	return out, nil
}

// outProperty returns the property the extraction is written to.
func (n *HTMLNode) outProperty() string {
	if n.config.OutProperty != "" {
		return n.config.OutProperty
	}
	if n.config.Property != "" {
		return n.config.Property
	}
	return "payload"
}

// extract renders one matched element per the configured ret mode.
// "html" produces the element's inner HTML — for an element whose only
// content is text that equals its text content.
func (n *HTMLNode) extract(m *xhtml.Node) interface{} {
	switch n.config.Ret {
	case "", "html":
		var b strings.Builder
		for c := m.FirstChild; c != nil; c = c.NextSibling {
			_ = xhtml.Render(&b, c)
		}
		return b.String()
	case "text":
		return nodeText(m)
	case "val":
		return attrOf(m, "value")
	default: // Node-RED: any other ret names an attribute
		return attrOf(m, n.config.Ret)
	}
}

// Expand turns stashed matches into one parts-tagged message per match
// (same parts {id,type,index,count} shape the split node produces so a
// downstream join can reassemble).
func (n *HTMLNode) Expand(output nodestore.MessageInterface) []nodestore.MessageInterface {
	if n.config.As != "multi" {
		return []nodestore.MessageInterface{output}
	}
	if len(n.matches) == 0 {
		return nil // no matches: nothing emitted (Node-RED sends nothing)
	}
	groupID := fmt.Sprintf("parts-%d", atomic.AddInt64(&htmlGroupSeq, 1))
	count := len(n.matches)
	out := make([]nodestore.MessageInterface, 0, count)
	for i, v := range n.matches {
		p := nodebase.ClonePayload(output)
		if p == nil {
			continue
		}
		if p.GetMetadata() == nil {
			p.SetMetadata(map[string]interface{}{})
		}
		nodebase.NodeSetProperty(p, n.outProperty(), v)
		parts := map[string]interface{}{
			"id":    groupID,
			"type":  "html",
			"index": i,
			"count": count,
		}
		if prev, ok := p.GetMetadata()["parts"]; ok {
			parts["parts"] = prev
		}
		p.GetMetadata()["parts"] = parts
		out = append(out, p)
	}
	return out
}

// Validate checks the html node configuration
func (n *HTMLNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Tag != "" {
		// An empty tag is allowed: the selector then comes from
		// msg.select at runtime.
		if _, err := parseCSSSelector(n.config.Tag); err != nil {
			return fmt.Errorf("html node: %w", err)
		}
	}
	switch n.config.As {
	case "", "single", "multi":
	default:
		return fmt.Errorf("invalid html \"as\" mode %q (single|multi)", n.config.As)
	}
	return nil
}

// nodeText returns the concatenated text content of n.
func nodeText(n *xhtml.Node) string {
	var b strings.Builder
	var walk func(*xhtml.Node)
	walk = func(c *xhtml.Node) {
		if c.Type == xhtml.TextNode {
			b.WriteString(c.Data)
		}
		for ch := c.FirstChild; ch != nil; ch = ch.NextSibling {
			walk(ch)
		}
	}
	walk(n)
	return b.String()
}

// attrOf returns the named attribute's value ("" when absent).
func attrOf(n *xhtml.Node, name string) string {
	for _, a := range n.Attr {
		if a.Key == name {
			return a.Val
		}
	}
	return ""
}

// --- minimal CSS selector host -------------------------------------
// Supports the selector subset the html node commonly needs:
//   tag, *, .class, #id, [attr], [attr=value]
// combined per compound selector (e.g. "div.note[data-x=1]"), the
// descendant and child (>) combinators, and comma-separated groups
// ("h1, .title").

// cssCompound is one compound selector: a tag ("" = any) plus
// class/id/attribute predicates, ANDed together.
type cssCompound struct {
	tag     string
	id      string
	classes []string
	attrs   map[string]string // attr name -> required value ("" = presence)
}

// cssStep is a compound plus the combinator joining it to the previous
// step: ' ' (descendant) or '>' (child).
type cssStep struct {
	combinator byte
	compound   cssCompound
}

// cssSelector is one selector chain; parseCSSSelector returns a slice
// of chains (comma groups).
type cssSelector []cssStep

// parseCSSSelector parses a selector string into chains.
func parseCSSSelector(s string) ([]cssSelector, error) {
	var out []cssSelector
	for _, group := range strings.Split(s, ",") {
		chain, err := parseCSSChain(strings.TrimSpace(group))
		if err != nil {
			return nil, err
		}
		if len(chain) > 0 {
			out = append(out, chain)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("empty selector")
	}
	return out, nil
}

// parseCSSChain parses one comma-free selector into steps.
func parseCSSChain(s string) (cssSelector, error) {
	var chain cssSelector
	combinator := byte(' ') // first step's combinator is unused
	i := 0
	for i < len(s) {
		// skip whitespace; a space between compounds is the
		// descendant combinator unless a '>' follows
		for i < len(s) && s[i] == ' ' {
			i++
			combinator = ' '
		}
		if i < len(s) && s[i] == '>' {
			combinator = '>'
			i++
			for i < len(s) && s[i] == ' ' {
				i++
			}
		}
		if i >= len(s) {
			break
		}
		start := i
		for i < len(s) && s[i] != ' ' && s[i] != '>' {
			if s[i] == '[' {
				// attribute selectors may contain spaces/'>':
				// consume to the closing bracket
				end := strings.IndexByte(s[i:], ']')
				if end < 0 {
					return nil, fmt.Errorf("invalid selector %q: unclosed [", s)
				}
				i += end + 1
				continue
			}
			i++
		}
		comp, err := parseCSSCompound(s[start:i])
		if err != nil {
			return nil, err
		}
		chain = append(chain, cssStep{combinator: combinator, compound: *comp})
		combinator = ' '
	}
	return chain, nil
}

// parseCSSCompound parses a compound like "div.note#main[a=1]".
func parseCSSCompound(s string) (*cssCompound, error) {
	c := &cssCompound{attrs: map[string]string{}}
	i := 0
	// optional leading tag or *
	if i < len(s) && (isCSSNameChar(s[i]) || s[i] == '*') {
		start := i
		for i < len(s) && isCSSNameChar(s[i]) {
			i++
		}
		if s[start] != '*' {
			c.tag = strings.ToLower(s[start:i])
		}
	}
	for i < len(s) {
		switch s[i] {
		case '.':
			i++
			start := i
			for i < len(s) && isCSSNameChar(s[i]) {
				i++
			}
			if start == i {
				return nil, fmt.Errorf("invalid selector %q: empty class", s)
			}
			c.classes = append(c.classes, s[start:i])
		case '#':
			i++
			start := i
			for i < len(s) && isCSSNameChar(s[i]) {
				i++
			}
			if start == i {
				return nil, fmt.Errorf("invalid selector %q: empty id", s)
			}
			c.id = s[start:i]
		case '[':
			end := strings.IndexByte(s[i:], ']')
			if end < 0 {
				return nil, fmt.Errorf("invalid selector %q: unclosed [", s)
			}
			body := s[i+1 : i+end]
			name, val := body, ""
			if eq := strings.IndexByte(body, '='); eq >= 0 {
				name = strings.TrimSpace(body[:eq])
				val = strings.Trim(strings.TrimSpace(body[eq+1:]), `"'`)
			}
			name = strings.TrimSpace(name)
			if name == "" {
				return nil, fmt.Errorf("invalid selector %q: empty attribute", s)
			}
			c.attrs[name] = val
			i += end + 1
		default:
			return nil, fmt.Errorf("invalid selector %q: unexpected %q", s, s[i])
		}
	}
	return c, nil
}

func isCSSNameChar(b byte) bool {
	return b == '-' || b == '_' ||
		(b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

// matches reports whether an element node satisfies the compound.
func (c *cssCompound) matches(n *xhtml.Node) bool {
	if n.Type != xhtml.ElementNode {
		return false
	}
	if c.tag != "" && !strings.EqualFold(n.Data, c.tag) {
		return false
	}
	if c.id != "" && attrOf(n, "id") != c.id {
		return false
	}
	for _, cls := range c.classes {
		if !hasClass(n, cls) {
			return false
		}
	}
	for name, val := range c.attrs {
		if val == "" {
			found := false
			for _, a := range n.Attr {
				if a.Key == name {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		} else if attrOf(n, name) != val {
			return false
		}
	}
	return true
}

func hasClass(n *xhtml.Node, cls string) bool {
	for _, c := range strings.Fields(attrOf(n, "class")) {
		if c == cls {
			return true
		}
	}
	return false
}

// findAll returns every element matching any of the selector chains,
// in document order, deduplicated.
func findAll(root *xhtml.Node, chains []cssSelector) []*xhtml.Node {
	var out []*xhtml.Node
	seen := map[*xhtml.Node]bool{}
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n.Type == xhtml.ElementNode && !seen[n] {
			for _, chain := range chains {
				if matchChain(n, chain, len(chain)-1) {
					seen[n] = true
					out = append(out, n)
					break
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return out
}

// matchChain reports whether n satisfies chain step idx, walking
// ancestors per the preceding combinator.
func matchChain(n *xhtml.Node, chain cssSelector, idx int) bool {
	if idx < 0 {
		return true
	}
	if !chain[idx].compound.matches(n) {
		return false
	}
	if idx == 0 {
		return true
	}
	rest := chain[:idx]
	switch chain[idx].combinator {
	case '>':
		for p := n.Parent; p != nil; p = p.Parent {
			if p.Type == xhtml.ElementNode {
				return matchChain(p, rest, len(rest)-1)
			}
		}
		return false
	default: // descendant
		for p := n.Parent; p != nil; p = p.Parent {
			if p.Type == xhtml.ElementNode && matchChain(p, rest, len(rest)-1) {
				return true
			}
		}
		return false
	}
}
