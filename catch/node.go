// Package catch contains the catch node implementation.
package catch

import (
	_ "embed"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

// factory builds the pass-through executor used for types with no
// payload logic.
func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	return nodebase.NewPassThroughNode(n), nil
}

// init registers the catch node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "catch",
			Name:        "Catch",
			Description: "Receives errors from nodes in scope",
			Icon:        "bug",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the catch node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"scope": map[string]interface{}{
			"type":        "string",
			"title":       "Scope",
			"enum":        []string{"all", "selected"},
			"default":     "all",
			"description": "all: receive errors from every node; selected: only from the listed nodes",
		},
		"nodes": map[string]interface{}{
			"type":        "string",
			"format":      "textarea",
			"title":       "Nodes",
			"description": "Comma-separated node ids to watch (only for scope \"selected\")",
		},
	},
}
