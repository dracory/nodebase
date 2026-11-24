// Package uitextinput contains the uiTextInput dashboard input
// widget: a text field whose enter/send emits the string as
// msg.payload. The executor is a pure pass-through — client
// interactions are injected downstream of the node by the dashboard
// hub (see nodes/uishare.go), so they never enter Execute.
package uitextinput

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

// init registers the uiTextInput node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "uiTextInput",
			Name:        "UI Text Input",
			Description: "Dashboard text field: enter/send emits the string as msg.payload",
			Icon:        "keyboard",
			Category:    "Dashboard",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the uiTextInput
// node. Manifest config: {"placeholder": "<string>"}.
var schema = map[string]interface{}{
	"type": "object",
	"properties": nodebase.UIWithLayout(map[string]interface{}{
		"placeholder": map[string]interface{}{
			"type":  "string",
			"title": "Placeholder",
		},
	}),
}
