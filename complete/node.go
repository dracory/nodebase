// Package complete contains the complete node implementation.
package complete

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

// init registers the complete node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "complete",
			Name:        "Complete",
			Description: "Fires when a node in scope finishes handling a message",
			Icon:        "check-circle",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the complete node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"scope": map[string]interface{}{
			"type":        "string",
			"title":       "Scope",
			"enum":        []string{"all", "selected"},
			"default":     "all",
			"description": "all: fire when any node's message finishes; selected: only for the listed nodes",
		},
		"nodes": map[string]interface{}{
			"type":        "string",
			"format":      "textarea",
			"title":       "Nodes",
			"description": "Comma-separated node ids to watch (only for scope \"selected\")",
		},
	},
}
