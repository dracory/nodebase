// Package aitoolreturn contains the ai-tool-return node
// implementation: the exit point of a flow exposed as an agent tool.
// It resolves the pending tool call the ai-agent awaits, delivering
// the processed message back as the tool result — the same
// correlation convention as linkOut's return mode.
package aitoolreturn

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/dracory/nodebase"
	"github.com/dracory/nodebase/aitool"
	nodestore "github.com/dracory/nodestore"
)

// Type is the registered node type id.
const Type = "ai-tool-return"

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(Config)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewReturnNode(n, c, e), nil
}

// init registers the ai-tool-return node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        Type,
			Name:        "AI Tool Return",
			Description: "Answers the calling ai-agent's pending tool call with the processed message",
			Icon:        "reply",
			Category:    "AI",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the ai-tool-return node.
var schema = map[string]interface{}{
	"type":       "object",
	"properties": map[string]interface{}{},
}

// Config holds the ai-tool-return configuration (currently none — the
// correlation is taken from the message's stamped id).
type Config struct {
}

// ReturnNode is the ai-tool-return executor.
type ReturnNode struct {
	*nodebase.BaseNode
	config     *Config
	host       nodebase.NodeHost
	workflowID string
}

// NewReturnNode creates a new ai-tool-return node.
func NewReturnNode(node nodestore.NodeInterface, config *Config, host nodebase.NodeHost) *ReturnNode {
	return &ReturnNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute resolves the pending call stamped into the message metadata
// ("msgId" — registered by the host when the agent injected the call)
// and passes the message through, so a wired output still receives it.
func (n *ReturnNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if n.host == nil {
		return nil, fmt.Errorf("ai-tool-return: no host — cannot resolve the pending tool call")
	}
	msgID, _ := input.GetMetadata()[aitool.MsgIDKey].(string)
	if msgID == "" {
		return nil, fmt.Errorf("ai-tool-return: caller message id unknown (message was not sent by an ai-agent)")
	}
	if err := n.host.ResolveResponse(msgID, input); err != nil {
		return nil, fmt.Errorf("ai-tool-return: %w", err)
	}
	return input, nil
}
