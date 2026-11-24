// Package template contains the template node implementation.
package template

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
	"text/template"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(TemplateConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewTemplateNode(n, c), nil
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
		"template": map[string]string{
			"type":        "string",
			"title":       "Template",
			"description": "Go text/template rendered against the payload (e.g. {\"echo\": \"{{.body.name}}\"})",
		},
		"format": map[string]interface{}{
			"type":    "string",
			"title":   "Output Format",
			"enum":    []string{"json", "text"},
			"default": "json",
		},
	},
	"required": []string{"template"},
}

// TemplateConfig holds configuration for the template node. Format is
// "json" (default — rendered output is parsed into msg.Payload) or
// "text" (rendered output becomes msg.Payload as a string).
type TemplateConfig struct {
	Template string `json:"template"`
	Format   string `json:"format"`
}

// TemplateNode renders a Go text/template against msg.Payload, the
// equivalent of Node-RED's template node
type TemplateNode struct {
	*nodebase.BaseNode
	config *TemplateConfig
}

// NewTemplateNode creates a new template node
func NewTemplateNode(node nodestore.NodeInterface, config *TemplateConfig) *TemplateNode {
	return &TemplateNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute renders the configured template with msg.Payload as the dot
// context and stores the result back into msg.Payload
func (n *TemplateNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	tpl, err := template.New(n.ID()).Parse(n.config.Template)
	if err != nil {
		return nil, fmt.Errorf("template parse error: %w", err)
	}

	var buf bytes.Buffer
	if err := tpl.Execute(&buf, input.GetPayload()); err != nil {
		return nil, fmt.Errorf("template execute error: %w", err)
	}

	if n.config.Format == "text" {
		input.SetPayload(buf.String())
		return input, nil
	}

	var data interface{}
	if err := json.Unmarshal(buf.Bytes(), &data); err != nil {
		return nil, fmt.Errorf("template output is not valid JSON: %w", err)
	}
	input.SetPayload(data)
	return input, nil
}
