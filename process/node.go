// Package process contains the process node implementation.
package process

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

// init registers the process node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "process",
			Name:        "",
			Description: "",
			Icon:        "",
			Category:    "",
			Set:         "core",
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}
