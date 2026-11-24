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
			"description": "List of {name, label?, type: text|number|checkbox|select, options?, value?}",
		},
	}),
}
