// Package linkout contains the linkOut node implementation.
package linkout

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(LinkOutConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewLinkOutNode(n, c, e), nil
}

// init registers the linkOut node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "linkOut",
			Name:        "Link Out",
			Description: "Pushes the message to Link In nodes, or returns it to a Link Call",
			Icon:        "link",
			Category:    "Flow Control",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the linkOut node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"links": map[string]interface{}{
			"type":        "array",
			"format":      "nodelist",
			"peerType":    "linkIn",
			"title":       "Links",
			"description": "linkIn nodes that receive pushed messages (same workflow)",
		},
		"mode": map[string]interface{}{
			"type":        "string",
			"title":       "Mode",
			"enum":        []string{"link", "return"},
			"default":     "link",
			"description": "link: push the message to every linked linkIn; return: answer the calling linkCall",
		},
	},
}

// UnmarshalJSON accepts links as either an array of strings or a
// comma-separated string.
func (c *LinkOutConfig) UnmarshalJSON(b []byte) error {
	var aux struct {
		Links interface{} `json:"links"`
		Mode  string      `json:"mode"`
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	c.Links = nodebase.ParseLinkList(aux.Links)
	c.Mode = aux.Mode
	return nil
}

// LinkOutNode pushes messages into linkIn nodes ("link" mode) or answers
// the pending linkCall that injected the message ("return" mode).
type LinkOutNode struct {
	*nodebase.BaseNode
	config     *LinkOutConfig
	host       nodebase.NodeHost
	workflowID string
}

// Execute pushes the message to every configured linkIn target (mode
// "link") or resolves the caller at the top of the link call stack
// (mode "return", consuming the message).
func (n *LinkOutNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if input == nil {
		return nil, fmt.Errorf("linkOut: nil input message")
	}
	mode := n.config.Mode
	if mode == "" {
		mode = "link"
	}

	if mode == "return" {
		return input, n.execReturn(input)
	}

	for _, target := range n.config.Links {
		// Node-RED pushes to linkIn nodes through the event bus: a
		// configured target that does not exist (dangling link) or is
		// not a linkIn simply has no listener — it is skipped, never
		// an error.
		if err := n.checkLinkInTarget(target); err != nil {
			continue
		}
		push := nodebase.ClonePayload(input)
		if push == nil {
			continue
		}
		if push.GetMetadata() == nil {
			push.SetMetadata(map[string]interface{}{})
		}
		push.GetMetadata()["linkSource"] = n.GetID()
		// emit seeds the edges FROM the linkIn node, so the pushed
		// clone travels through the link in's downstream wiring
		if err := n.host.Emit(n.workflowID, target, push, nil); err != nil {
			return nil, fmt.Errorf("linkOut: cannot reach link %q: %w", target, err)
		}
	}

	// mode "link": the message also continues downstream normally
	return input, nil
}

// execReturn pops the top link call stack entry and resolves the
// linkCall's pending message, delivering the processed payload back.
func (n *LinkOutNode) execReturn(input nodestore.MessageInterface) error {
	stack, _ := input.GetMetadata()[nodebase.LinkCallStackKey].([]interface{})
	if len(stack) == 0 {
		return fmt.Errorf("linkOut return: link call stack is empty (message was not sent by a linkCall)")
	}
	top, _ := stack[len(stack)-1].(map[string]interface{})
	input.GetMetadata()[nodebase.LinkCallStackKey] = stack[:len(stack)-1]

	msgID, _ := top["msgId"].(string)
	if msgID == "" {
		// Entries pushed before the injection returns carry no msgId
		// yet; the injected message's own msgId is the pending one
		msgID, _ = input.GetMetadata()["msgId"].(string)
	}
	if msgID == "" {
		return fmt.Errorf("linkOut return: caller message id unknown")
	}
	return n.host.ResolveResponse(msgID, input)
}

// checkLinkInTarget verifies a link target exists and is a linkIn node
// in the same workflow.
func (n *LinkOutNode) checkLinkInTarget(target string) error {
	node, ok := n.host.LookupNode(n.workflowID, target)
	if !ok || node == nil {
		return fmt.Errorf("linkOut: link target %q not found in workflow", target)
	}
	if node.GetType() != "linkIn" {
		return fmt.Errorf("linkOut: target %q is a %q node, not linkIn", target, node.GetType())
	}
	return nil
}

// LinkOutConfig holds configuration for the linkOut node
type LinkOutConfig struct {
	// Links are the ids of linkIn nodes that receive pushed messages
	Links []string `json:"links"`
	// Mode is "link" (push to linkIn nodes) or "return" (answer a linkCall)
	Mode string `json:"mode"`
}

// NewLinkOutNode creates a new linkOut node
func NewLinkOutNode(node nodestore.NodeInterface, config *LinkOutConfig, host nodebase.NodeHost) *LinkOutNode {
	return &LinkOutNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute pushes the message to every configured linkIn target (mode
// "link") or resolves the caller at the top of the link call stack
// (mode "return", consuming the message).
