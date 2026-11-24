// Package uibutton contains the uiButton dashboard input widget: a
// button whose click emits the configured payload downstream. The
// executor is a pure pass-through — client interactions are injected
// downstream of the node by the dashboard hub (see nodes/uishare.go),
// so they never enter Execute.
package uibutton

import (
	_ "embed"

	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	return nodebase.NewPassThroughNode(n), nil
}

// init registers the uiButton node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "uiButton",
			Name:        "UI Button",
			Description: "Dashboard button: click emits the configured payload downstream",
			Icon:        "hand-pointer",
			Category:    "Dashboard",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the uiButton node.
// Manifest config the client renderer consumes: {"payload": <any>}.
var schema = map[string]interface{}{
	"type": "object",
	"properties": nodebase.UIWithLayout(map[string]interface{}{
		"payload": map[string]interface{}{
			"type":        "string",
			"title":       "Payload",
			"default":     "clicked",
			"description": "Value emitted as msg.payload on click (JSON scalar)",
		},
	}),
}
