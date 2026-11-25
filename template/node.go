// Package template contains the template node implementation.
package template

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"text/template"

	"github.com/cbroglie/mustache"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"gopkg.in/yaml.v3"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(TemplateConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewTemplateNode(n, c, e), nil
}

// init registers the template node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "template",
			Name:        "Template",
			Description: "Renders a template against the payload",
			Icon:        "file-alt",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the template node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"syntax": map[string]interface{}{
			"type":        "string",
			"title":       "Syntax",
			"enum":        []string{"mustache", "gotemplate", "plain"},
			"default":     "mustache",
			"description": "mustache = Node-RED parity; gotemplate = Go text/template (extension-only); plain = no rendering",
		},
		"template": map[string]interface{}{
			"type":        "string",
			"title":       "Template",
			"description": "Template rendered against the message (e.g. {\"echo\": \"{{payload.body.name}}\"})",
		},
		"field": map[string]interface{}{
			"type":        "string",
			"title":       "Property",
			"default":     "payload",
			"description": "Message property (dotted path) or context key the result is written to",
		},
		"fieldType": map[string]interface{}{
			"type":    "string",
			"title":   "Set",
			"enum":    []string{"msg", "flow", "global"},
			"default": "msg",
		},
		"output": map[string]interface{}{
			"type":    "string",
			"title":   "Output Format",
			"enum":    []string{"str", "json", "yaml"},
			"default": "str",
		},
	},
	"required": []string{"template"},
}

// TemplateConfig holds configuration for the template node, mirroring
// Node-RED's template node fields plus the extension-only gotemplate
// syntax and the legacy Format alias.
type TemplateConfig struct {
	// Template is the template source; when empty, a runtime
	// msg.template metadata property supplies it (Node-RED parity).
	Template string `json:"template"`
	// Syntax: "mustache" (default, Node-RED parity), "gotemplate"
	// (extension-only Go text/template against msg.payload), or "plain"
	// (no rendering — the template text passes through verbatim).
	Syntax string `json:"syntax"`
	// Field is the target property: "payload" (default) or a dotted
	// message-property path when FieldType is "msg", or a context key
	// (optionally "#:(store)::key") when FieldType is flow|global.
	Field string `json:"field"`
	// FieldType: "msg" (default), "flow" or "global" — where the
	// rendered value is written.
	FieldType string `json:"fieldType"`
	// Output: "str" (default), "json" (parse the rendered text into
	// JSON), or "yaml" (parse into a YAML value).
	Output string `json:"output"`
	// Format is the legacy engine alias: "json" → Output "json",
	// "text" → Output "str". Ignored when Output is set.
	Format string `json:"format"`
}

// TemplateNode renders a Mustache template against the message
// (Node-RED parity), with Go text/template kept as a extension-only
// syntax option.
type TemplateNode struct {
	*nodebase.BaseNode
	host       nodebase.NodeHost
	workflowID string
	config     *TemplateConfig
}

// NewTemplateNode creates a new template node. host may be nil — the
// {{flow.X}} / {{global.X}} / {{env.X}} context tags then resolve to
// empty and flow/global field targets are dropped.
func NewTemplateNode(node nodestore.NodeInterface, config *TemplateConfig, host nodebase.NodeHost) *TemplateNode {
	return &TemplateNode{
		BaseNode:   nodebase.NewBaseNode(node),
		host:       host,
		workflowID: node.GetWorkflowID(),
		config:     config,
	}
}

// contextTag matches {{flow.X}}, {{flow[store].X}}, {{global.X}} and
// {{global[store].X}} — Node-RED's parseContext.
var contextTagRe = regexp.MustCompile(`^(flow|global)(\[(\w+)\])?\.(.+)`)

// envTagRe matches {{env.X}} — Node-RED's parseEnv.
var envTagRe = regexp.MustCompile(`^env\.(.+)`)

// contextFieldRe parses "#:(store)::key" field targets for flow/global
// writes (RED.util.parseContextStore).
var contextFieldRe = regexp.MustCompile(`^#:\((\w+)\)::(.+)`)

// Execute renders the template and writes the result to the configured
// target.
func (n *TemplateNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	tpl := n.config.Template
	if tpl == "" {
		// Node-RED: inbound msg.template is used only when the node has
		// no configured template.
		if mt, ok := input.GetMetadata()["template"].(string); ok {
			tpl = mt
		}
	}

	var value string
	switch n.syntax() {
	case "gotemplate":
		out, err := n.renderGoTemplate(tpl, input)
		if err != nil {
			return nil, err
		}
		value = out
	case "plain":
		value = tpl
	default: // "mustache" and any unset/unknown value — Node-RED parity
		out, err := n.renderMustache(tpl, input)
		if err != nil {
			return nil, err
		}
		value = out
	}

	parsed, err := n.parseOutput(value)
	if err != nil {
		return nil, err
	}
	n.writeTarget(input, parsed)
	return input, nil
}

// syntax resolves the effective syntax. "mustache" is the default for
// parity with Node-RED imported flows. Pre-mustache flows carry
// the legacy "format" field but no "syntax"; they keep the Go
// text/template behaviour they were saved with.
func (n *TemplateNode) syntax() string {
	if n.config.Syntax != "" {
		return n.config.Syntax
	}
	if n.config.Format != "" {
		return "gotemplate"
	}
	return "mustache"
}

// outputFormat resolves the effective output format: explicit Output
// wins; the legacy Format alias maps json→json and text→str.
func (n *TemplateNode) outputFormat() string {
	if n.config.Output != "" {
		return n.config.Output
	}
	switch n.config.Format {
	case "json":
		return "json"
	case "text":
		return "str"
	}
	return "str"
}

// parseOutput converts the rendered string per the output format —
// Node-RED's JSON.parse / yaml.load on the rendered value.
func (n *TemplateNode) parseOutput(value string) (interface{}, error) {
	switch n.outputFormat() {
	case "json":
		var data interface{}
		if err := json.Unmarshal([]byte(value), &data); err != nil {
			return nil, fmt.Errorf("template output is not valid JSON: %w", err)
		}
		return data, nil
	case "yaml":
		var data interface{}
		if err := yaml.Unmarshal([]byte(value), &data); err != nil {
			return nil, fmt.Errorf("template output is not valid YAML: %w", err)
		}
		return data, nil
	}
	return value, nil
}

// writeTarget stores the rendered value at the configured field:
// msg scope writes to payload or a dotted metadata path; flow/global
// write into the host context store.
func (n *TemplateNode) writeTarget(input nodestore.MessageInterface, value interface{}) {
	field := n.config.Field
	if field == "" {
		field = "payload"
	}
	switch n.config.FieldType {
	case "flow", "global":
		if n.host == nil {
			return
		}
		key, store := parseContextField(field)
		scope, scopeID := nodebase.ContextScopeFlow, n.workflowID
		if n.config.FieldType == "global" {
			scope, scopeID = nodebase.ContextScopeGlobal, ""
		}
		_ = store // named persistable stores unsupported: single store
		_ = n.host.ResolvedContextStore().SetContext(scope, scopeID, key, value)
	default: // "msg"
		if field == "payload" {
			input.SetPayload(value)
			return
		}
		if input.GetMetadata() == nil {
			input.SetMetadata(map[string]interface{}{})
		}
		nodebase.SetPath(input.GetMetadata(), field, value)
	}
}

// parseContextField splits a "#:(store)::key" context field; without
// the prefix the whole field is the key.
func parseContextField(field string) (key, store string) {
	if m := contextFieldRe.FindStringSubmatch(field); m != nil {
		return m[2], m[1]
	}
	return field, ""
}

// renderGoTemplate renders Go text/template syntax against
// msg.payload (extension-only syntax; original behaviour).
func (n *TemplateNode) renderGoTemplate(tpl string, input nodestore.MessageInterface) (string, error) {
	t, err := template.New(n.ID()).Parse(tpl)
	if err != nil {
		return "", fmt.Errorf("template parse error: %w", err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, input.GetPayload()); err != nil {
		return "", fmt.Errorf("template execute error: %w", err)
	}
	return buf.String(), nil
}

// renderMustache renders the template the way Node-RED does — see
// RenderMustache, which this wraps with the node's host, workflow and
// JSON-escaping behaviour.
func (n *TemplateNode) renderMustache(tpl string, input nodestore.MessageInterface) (string, error) {
	return RenderMustache(tpl, input, n.host, n.workflowID, n.GetID(), n.outputFormat() == "json")
}

// RenderMustache renders a mustache template the way Node-RED does:
// the message itself (payload + every metadata property) is the
// mustache context; {{env.X}}, {{flow.X}} and {{global.X}} tags are
// resolved against the host before rendering — the two-phase
// NodeContext behaviour, where context tokens are pre-resolved and
// message properties take precedence. host may be nil — context tags
// then resolve to empty. jsonEscape applies Node-RED's JSON-mode
// string escaping to the message context. Shared by nodes that
// template user text against the message (e.g. ai-request).
func RenderMustache(tpl string, input nodestore.MessageInterface, host nodebase.NodeHost, workflowID, nodeID string, jsonEscape bool) (string, error) {
	tmpl, err := mustache.ParseString(tpl)
	if err != nil {
		return "", fmt.Errorf("template parse error: %w", err)
	}

	// The mustache context is the whole message: payload plus every
	// metadata property at top level (msg.topic, msg.template, …).
	msgCtx := map[string]interface{}{}
	for k, v := range input.GetMetadata() {
		msgCtx[k] = v
	}
	msgCtx["payload"] = input.GetPayload()

	// Node-RED escapes strings from the message context when the
	// output mode is JSON so rendered output stays valid JSON.
	if jsonEscape {
		msgCtx = escapeStringsDeep(msgCtx).(map[string]interface{})
	}

	ctx := map[string]interface{}{}
	for k, v := range msgCtx {
		ctx[k] = v
	}
	resolveContextTags(tmpl, ctx, host, workflowID, nodeID)

	rendered, err := tmpl.Render(ctx)
	if err != nil {
		return "", fmt.Errorf("template render error: %w", err)
	}
	return rendered, nil
}

// resolveContextTags walks the parsed template's tags (mirroring
// Node-RED's extractTokens) and injects resolved env/flow/global
// tokens into the render context as nested maps, so e.g. {{env.HOME}}
// resolves ctx["env"]["HOME"]. Named persistable stores
// (flow[store].X / global[store].X) are not supported — the single
// host context store backs unprefixed lookups only, and named-store
// tokens stay unresolved (rendering empty, matching a missing key).
// Message properties win: when the message already carries a
// flow/global/env key of a different shape it is left alone.
func resolveContextTags(tmpl *mustache.Template, ctx map[string]interface{}, host nodebase.NodeHost, workflowID, nodeID string) {
	for _, name := range tagNames(tmpl.Tags()) {
		if m := envTagRe.FindStringSubmatch(name); m != nil {
			if v, ok := lookupEnv(host, workflowID, nodeID, m[1]); ok {
				setNested(ctx, "env", m[1], v)
			}
			continue
		}
		if m := contextTagRe.FindStringSubmatch(name); m != nil {
			if m[3] != "" {
				continue // named persistable store — unsupported
			}
			scope, scopeID := nodebase.ContextScopeFlow, workflowID
			if m[1] == "global" {
				scope, scopeID = nodebase.ContextScopeGlobal, ""
			}
			if v, ok := lookupContext(host, scope, scopeID, m[4]); ok {
				setNested(ctx, m[1], m[4], v)
			}
		}
	}
}

// tagNames flattens tag names recursively — like Node-RED's
// extractTokens, section children are included.
func tagNames(tags []mustache.Tag) []string {
	var names []string
	for _, t := range tags {
		names = append(names, t.Name())
		if t.Type() == mustache.Section || t.Type() == mustache.InvertedSection {
			names = append(names, tagNames(t.Tags())...)
		}
	}
	return names
}

// setNested writes value at ctx[scope][dotted key], creating the scope
// map unless the message already claims that key in another shape.
func setNested(ctx map[string]interface{}, scope, key string, value interface{}) {
	cur, ok := ctx[scope].(map[string]interface{})
	if !ok {
		if _, exists := ctx[scope]; exists {
			return // message property shadows the context scope
		}
		cur = map[string]interface{}{}
		ctx[scope] = cur
	}
	nodebase.SetPath(cur, key, value)
}

// lookupEnv resolves an environment variable the way the function
// node's env.get does: host NodeEnv first, then the OS environment
// when the host opts in via OSEnvAllowed.
func lookupEnv(host nodebase.NodeHost, workflowID, nodeID, name string) (interface{}, bool) {
	if host == nil {
		return nil, false
	}
	if v, ok := host.NodeEnv(workflowID, nodeID, name); ok {
		return v, true
	}
	g, ok := host.(interface{ OSEnvAllowed() bool })
	if !ok || !g.OSEnvAllowed() {
		return nil, false
	}
	if v, ok := os.LookupEnv(name); ok {
		return v, true
	}
	return nil, false
}

// lookupContext reads a key from the host context store; nil host or
// store error resolves to absent.
func lookupContext(host nodebase.NodeHost, scope, scopeID, key string) (interface{}, bool) {
	if host == nil {
		return nil, false
	}
	v, ok, err := host.ResolvedContextStore().GetContext(scope, scopeID, key)
	if err != nil || !ok {
		return nil, false
	}
	return v, true
}

// escapeStringsDeep applies Node-RED's JSON-mode escaping to every
// string in the message context: backslash, newline, tab, carriage
// return, form feed and backspace are escaped so mustache output can
// sit inside a JSON string literal.
func escapeStringsDeep(v interface{}) interface{} {
	switch t := v.(type) {
	case string:
		return escapeJSONString(t)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, e := range t {
			out[k] = escapeStringsDeep(e)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = escapeStringsDeep(e)
		}
		return out
	}
	return v
}

var jsonEscaper = strings.NewReplacer(
	`\`, `\\`,
	"\n", `\n`,
	"\t", `\t`,
	"\r", `\r`,
	"\f", `\f`,
	"\b", `\b`,
)

func escapeJSONString(s string) string { return jsonEscaper.Replace(s) }
