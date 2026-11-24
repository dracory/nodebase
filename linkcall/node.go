// Package linkcall contains the linkCall node implementation.
package linkcall

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(LinkCallConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewLinkCallNode(n, c, e), nil
}

// init registers the linkCall node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "linkCall",
			Name:        "Link Call",
			Description: "Calls a Link In subflow and awaits its Link Out return",
			Icon:        "link",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the linkCall node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"link": map[string]interface{}{
			"type":        "string",
			"format":      "nodeselect",
			"peerType":    "linkIn",
			"title":       "Link",
			"description": "Target linkIn node (static mode)",
		},
		"linkType": map[string]interface{}{
			"type":        "string",
			"title":       "Link Type",
			"enum":        []string{"static", "dynamic"},
			"default":     "static",
			"description": "static: use the configured link; dynamic: take the target from message metadata \"target\"",
		},
		"timeout": map[string]interface{}{
			"type":        "number",
			"title":       "Timeout (seconds)",
			"description": "How long to await the linkOut return; 0 = 30 seconds",
			"default":     30,
		},
	},
}

// LinkCallConfig holds configuration for the linkCall node
type LinkCallConfig struct {
	// Link is the id of the target linkIn node (linkType "static")
	Link string `json:"link"`
	// LinkType is "static" (configured link) or "dynamic" (target taken
	// from the message's target metadata)
	LinkType string `json:"linkType"`
	// Timeout in seconds to await the linkOut return (default 30)
	Timeout float64 `json:"timeout"`
}

// NewLinkCallNode creates a new linkCall node
func NewLinkCallNode(node nodestore.NodeInterface, config *LinkCallConfig, host nodebase.NodeHost) *LinkCallNode {
	return &LinkCallNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute resolves the target linkIn, pushes this node onto the link
// call stack, injects the message and waits for the linkOut return.
// The returned payload becomes this node's output.
func (n *LinkCallNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	target := n.config.Link
	if n.config.LinkType == "dynamic" {
		if v, ok := input.GetMetadata()["target"].(string); ok && v != "" {
			target = v
		}
	}
	if target == "" {
		return nil, fmt.Errorf("linkCall: no link target configured")
	}
	node, ok := n.host.LookupNode(n.workflowID, target)
	if !ok {
		return nil, fmt.Errorf("linkCall: link target %q not found in workflow", target)
	}
	if node.GetType() != "linkIn" {
		return nil, fmt.Errorf("linkCall: target %q is a %q node, not linkIn", target, node.GetType())
	}

	// Push this call onto the message's link call stack. The entry's
	// msgId is intentionally left empty — writing it here (after
	// InjectMessage returns) would race the injected traversal, which
	// clones the metadata on a separate goroutine. linkOut's
	// execReturn falls back to the injected message's stamped "msgId"
	// metadata key, which is the same pending correlation id.
	if input.GetMetadata() == nil {
		input.SetMetadata(map[string]interface{}{})
	}
	stack, _ := input.GetMetadata()[nodebase.LinkCallStackKey].([]interface{})
	entry := map[string]interface{}{"nodeId": n.GetID()}
	input.GetMetadata()[nodebase.LinkCallStackKey] = append(stack, entry)

	msgID, ch, err := n.host.InjectMessage(n.workflowID, target, input)
	if err != nil {
		return nil, fmt.Errorf("linkCall: cannot reach link %q: %w", target, err)
	}

	timeout := nodebase.DefaultLinkCallTimeout
	if n.config.Timeout > 0 {
		timeout = time.Duration(n.config.Timeout * float64(time.Second))
	}

	select {
	case response := <-ch:
		return response, nil
	case <-time.After(timeout):
		n.host.DiscardPending(msgID)
		return nil, fmt.Errorf("linkCall: no response from link %q within %v — is a linkOut(return) wired downstream?", target, timeout)
	case <-ctx.Done():
		n.host.DiscardPending(msgID)
		return nil, ctx.Err()
	}
}

// LinkCallNode injects the message into a target linkIn and awaits the
// matching linkOut(return), implementing RPC-style request/response.
type LinkCallNode struct {
	*nodebase.BaseNode
	config     *LinkCallConfig
	host       nodebase.NodeHost
	workflowID string
}
