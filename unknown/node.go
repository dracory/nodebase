// Package unknown contains the unknown node implementation.
package unknown

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"sync"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	return NewUnknownNode(n, e), nil
}

// init registers the unknown node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "unknown",
			Name:        "Unknown",
			Description: "Placeholder for unrecognized node types; excluded from execution",
			Icon:        "question-circle",
			Category:    "Common",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the unknown node.
var schema = map[string]interface{}{
	"type":        "object",
	"description": "Placeholder for unrecognized node types: keeps its raw config and is excluded from execution with a warning",
	"properties":  map[string]interface{}{},
}

// UnknownNode is the executor for node types the engine does not implement
// (e.g. an imported Node-RED contrib node, or the explicit "unknown"
// palette type). Node-RED parity: the node loads visibly marked, keeps
// its raw config so a save/export round-trips it verbatim, and is
// excluded from execution — a message that reaches it dies at the node
// and a warning lands in the event log (once per run).
type UnknownNode struct {
	*nodebase.BaseNode
	host       nodebase.NodeHost
	workflowID string
	warnOnce   sync.Once
}

// NewUnknownNode creates an excluded executor for an unrecognized type.

func NewUnknownNode(node nodestore.NodeInterface, host nodebase.NodeHost) *UnknownNode {
	return &UnknownNode{
		BaseNode:   nodebase.NewBaseNode(node),
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute emits a one-time warning and drops the message: an unknown
// node cannot run, and Node-RED semantics exclude it from the flow
// rather than silently passing data through an unimplemented node.

func (n *UnknownNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.warnOnce.Do(func() {
		if n.host != nil {
			n.host.EmitEvent(n.workflowID, n.GetID(), n.GetID(), "warn",
				fmt.Sprintf("unknown node type %q excluded from execution", n.Type()))
		}
	})
	return nil, nil
}

// Validate always passes — an unrecognized config is preserved
// verbatim, never judged.

func (n *UnknownNode) Validate() error {
	return n.BaseNode.Validate()
}
