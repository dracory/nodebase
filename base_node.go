package nodebase

import (
	"context"
	"fmt"

	nodestore "github.com/dracory/nodestore"
)

// BaseNode provides the shared executor behavior every node type
// builds on. It carries no identity of its own: it wraps the
// nodestore.NodeInterface entity, so the node's id, type, position and
// config Data live in exactly one place — the executor is a behavior
// shell over the entity, not a shadow copy of it.
type BaseNode struct {
	nodestore.NodeInterface
}

// NewBaseNode creates a new BaseNode wrapping node.
func NewBaseNode(node nodestore.NodeInterface) *BaseNode {
	return &BaseNode{NodeInterface: node}
}

// ID returns the node's ID (NodeExecutor contract over the entity).
func (n *BaseNode) ID() string {
	return n.GetID()
}

// Type returns the node's type (NodeExecutor contract over the entity).
func (n *BaseNode) Type() string {
	return n.GetType()
}

// Execute is a default implementation that can be overridden by specific nodes
func (n *BaseNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	// Default implementation just passes through the input
	// Specific node types should override this method
	return input, nil
}

// Validate performs basic validation that all nodes should pass
func (n *BaseNode) Validate() error {
	if n.GetID() == "" {
		return fmt.Errorf("node ID cannot be empty")
	}
	if n.GetType() == "" {
		return fmt.Errorf("node type cannot be empty")
	}
	return nil
}
