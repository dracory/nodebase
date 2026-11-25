// Package websocketlistener contains the websocketListener config
// node — the shared websocket endpoint description websocketIn /
// websocketOut reference via their "server" field (Node-RED
// websocket-listener). The runtime half (the mounted upgrade route
// plus accepted connections) is nodebase.WSListenerState, resolved
// through nodebase.ResolveWSListener.
package websocketlistener

import (
	"context"
	_ "embed"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(nodebase.WSListenerConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewWebSocketListenerNode(n, c), nil
}

// init registers the websocketListener node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        nodebase.WebSocketListenerType,
			Name:        "WS Listener",
			Description: "Shared websocket listener endpoint referenced by websocketIn/websocketOut nodes",
			Icon:        "plug",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the
// websocketListener config node. Field spellings follow the Node-RED
// websocket-listener export format.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"path": map[string]interface{}{
			"type":        "string",
			"title":       "Path",
			"description": "Endpoint path served on the host's shared HTTP listener",
			"default":     "/ws",
		},
		"wholemsg": map[string]interface{}{
			"type":    "string",
			"title":   "Whole Message",
			"enum":    []string{"false", "true"},
			"default": "false",
		},
	},
}

// WebSocketListenerNode is a config entity; its Execute is never
// invoked (no ports), existing only to satisfy the NodeExecutor
// contract.
type WebSocketListenerNode struct {
	*nodebase.BaseNode
	config *nodebase.WSListenerConfig
}

// Execute is inert — config nodes carry no message flow.
func (n *WebSocketListenerNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// NewWebSocketListenerNode creates a new websocketListener node
func NewWebSocketListenerNode(node nodestore.NodeInterface, config *nodebase.WSListenerConfig) *WebSocketListenerNode {
	return &WebSocketListenerNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}
