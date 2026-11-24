// Package comment contains the comment node implementation.
package comment

import (
	"context"
	_ "embed"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(CommentConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewCommentNode(n, c), nil
}

// init registers the comment node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "comment",
			Name:        "Comment",
			Description: "Canvas annotation text; never executes",
			Icon:        "comment",
			Category:    "Common",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the comment node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"info": map[string]interface{}{
			"type":        "string",
			"format":      "textarea",
			"title":       "Comment",
			"description": "Annotation text rendered on the canvas; the node never executes",
		},
	},
}

// CommentConfig holds configuration for the comment node — the
// Node-RED `comment` equivalent. It carries only annotation text and
// has no runtime behaviour; the node deploys and round-trips but is
// never wired, so Execute simply passes the (never-delivered) payload.
type CommentConfig struct {
	// Info is the free-form annotation text shown on the canvas
	// (Node-RED field name: info).
	Info string `json:"info"`
}

// CommentNode is a metadata-only flow annotation (Node-RED `comment`).
// It renders in the editor and keeps its text in the flow export, but
// it has no inputs/outputs and never executes.
type CommentNode struct {
	*nodebase.BaseNode
	config *CommentConfig
}

// NewCommentNode creates a new comment node
func NewCommentNode(node nodestore.NodeInterface, config *CommentConfig) *CommentNode {
	return &CommentNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute is a no-op pass-through: a comment node has no ports, so no
// message ever reaches it; the implementation exists to satisfy the
// NodeExecutor contract.
func (n *CommentNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// Validate always passes — an annotation cannot be misconfigured.
func (n *CommentNode) Validate() error {
	return n.BaseNode.Validate()
}
