// Package uiform contains the uiForm dashboard input widget: a form
// of named fields whose submit emits an object msg.payload
// ({fieldName: value}). The executor is a pure pass-through — client
// interactions are injected downstream of the node by the dashboard
// hub (see nodes/uishare.go), so they never enter Execute.
package uiform

import (
	_ "embed"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	return nodebase.NewPassThroughNode(n), nil
}

// init registers the uiForm node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "uiForm",
			Name:        "UI Form",
			Description: "Dashboard form: named fields + submit emits an object msg.payload",
			Icon:        "wpforms",
			Category:    "Dashboard",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the uiForm node.
// Manifest config: {"fields": [{"name","label","type","options",
// "value"}...]} with type ∈ text|number|checkbox|select.
var schema = map[string]interface{}{
	"type": "object",
	"properties": nodebase.UIWithLayout(map[string]interface{}{
		"fields": map[string]interface{}{
			"type":        "array",
			"title":       "Fields",
			"description": "Form controls rendered on the dashboard; submit emits an object payload keyed by each field Name",
			"items": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name": map[string]string{
						"type":  "string",
						"title": "Name",
					},
					"label": map[string]string{
						"type":  "string",
						"title": "Label",
					},
					"type": map[string]interface{}{
						"type":    "string",
						"title":   "Type",
						"enum":    []string{"text", "number", "checkbox", "select"},
						"default": "text",
					},
					"options": map[string]interface{}{
						"type":        "string",
						"title":       "Options",
						"description": "Comma-separated choices for select fields",
						"showWhen":    map[string]interface{}{"key": "type", "in": []string{"select"}},
					},
					"value": map[string]interface{}{
						"type":            "string",
						"title":           "Initial Value",
						"format":          "typedinput",
						"typeField":       "valueType",
						"typedInputTypes": []string{"str", "num", "bool", "json"},
						"defaultType":     "str",
					},
				},
			},
		},
	}),
}
