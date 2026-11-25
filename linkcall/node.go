// Package linkcall contains the linkCall node implementation.
package linkcall

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"strconv"
	"time"
)

// flexNumber is a float64 that also accepts Node-RED's string spelling
// ("30") in serialized configs.
type flexNumber float64

// UnmarshalJSON accepts both a number and a numeric string.
func (f *flexNumber) UnmarshalJSON(data []byte) error {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch t := v.(type) {
	case float64:
		*f = flexNumber(t)
	case string:
		n, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return err
		}
		*f = flexNumber(n)
	}
	return nil
}

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
	// Timeout in seconds to await the linkOut return (default 30).
	// Node-RED serializes this as a string ("30") — flexNumber accepts
	// both spellings.
	Timeout flexNumber `json:"timeout"`
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

// Execute resolves the target linkIn — by id in static mode, by id or
// unique linkIn name in dynamic mode (Node-RED's linkcallTargets
// semantics) — injects the message and waits for the linkOut return.
// The returned payload becomes this node's output.
func (n *LinkCallNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	target := n.config.Link
	dynamic := n.config.LinkType == "dynamic"
	if dynamic {
		if v, ok := input.GetMetadata()["target"].(string); ok && v != "" {
			target = v
		}
	}
	if target == "" {
		return nil, fmt.Errorf("linkCall: no link target configured")
	}
	targetID, err := nodebase.ResolveLinkInTarget(n.host, n.workflowID, target, dynamic)
	if err != nil {
		return nil, err
	}

	timeout := nodebase.DefaultLinkCallTimeout
	if n.config.Timeout > 0 {
		timeout = time.Duration(float64(n.config.Timeout) * float64(time.Second))
	}
	return nodebase.LinkCallInto(n.host, n.workflowID, n.GetID(), targetID, input, timeout)
}

// LinkCallNode injects the message into a target linkIn and awaits the
// matching linkOut(return), implementing RPC-style request/response.
type LinkCallNode struct {
	*nodebase.BaseNode
	config     *LinkCallConfig
	host       nodebase.NodeHost
	workflowID string
}
