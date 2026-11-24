// Package linkin contains the linkIn node implementation.
package linkin

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

// init registers the linkIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "linkIn",
			Name:        "Link In",
			Description: "Receives messages pushed by Link Out and Link Call nodes",
			Icon:        "link",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the linkIn node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"links": map[string]interface{}{
			"type":        "array",
			"format":      "nodelist",
			"peerType":    "linkOut",
			"title":       "Links",
			"description": "linkOut nodes allowed to push messages into this link",
		},
	},
}
