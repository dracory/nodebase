package nodebase

import (
	"context"

	nodestore "github.com/dracory/nodestore"
)

// PassThroughNode is the executor for node types without payload
// logic (junction, event sources, legacy internal types).
type PassThroughNode struct {
	*BaseNode
}

// NewPassThroughNode creates the pass-through executor for node.
func NewPassThroughNode(node nodestore.NodeInterface) *PassThroughNode {
	return &PassThroughNode{BaseNode: NewBaseNode(node)}
}

// Execute returns the input unchanged.
func (n *PassThroughNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}
