// Package junction contains the junction node implementation.
package junction

import (
	_ "embed"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

//go:embed editor.html
var editorHTML string

// factory builds the pass-through executor used for types with no
// payload logic.
func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	return nodebase.NewPassThroughNode(n), nil
}

// init registers the junction node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "junction",
			Name:        "Junction",
			Description: "A pass-through bend point for organising wires",
			Icon:        "circle",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the junction node.
var schema = map[string]interface{}{
	"type":       "object",
	"properties": map[string]interface{}{},
}
