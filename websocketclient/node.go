// Package websocketclient contains the websocketClient config node —
// the shared outbound websocket connection description websocketIn /
// websocketOut reference via their "client" field (Node-RED
// websocket-client). The runtime half (the dialed connection) is
// nodebase.WSClientState, resolved through nodebase.ResolveWSClient.
package websocketclient

import (
	"context"
	_ "embed"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(nodebase.WSClientConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewWebSocketClientNode(n, c), nil
}

// init registers the websocketClient node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        nodebase.WebSocketClientType,
			Name:        "WS Client",
			Description: "Shared outbound websocket connection referenced by websocketIn/websocketOut nodes",
			Icon:        "plug",
			Category:    "Network",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the websocketClient
// config node. Field spellings follow the Node-RED websocket-client
// export format.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"path": map[string]interface{}{
			"type":        "string",
			"title":       "URL",
			"description": "Full ws:// or wss:// URL to connect to",
		},
		"tls": map[string]interface{}{
			"type":        "string",
			"title":       "TLS Config",
			"description": "Optional tlsConfig node id for wss connections",
		},
		"wholemsg": map[string]interface{}{
			"type":    "string",
			"title":   "Whole Message",
			"enum":    []string{"false", "true"},
			"default": "false",
		},
	},
}

// WebSocketClientNode is a config entity; its Execute is never
// invoked (no ports), existing only to satisfy the NodeExecutor
// contract.
type WebSocketClientNode struct {
	*nodebase.BaseNode
	config *nodebase.WSClientConfig
}

// Execute is inert — config nodes carry no message flow.
func (n *WebSocketClientNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// NewWebSocketClientNode creates a new websocketClient node
func NewWebSocketClientNode(node nodestore.NodeInterface, config *nodebase.WSClientConfig) *WebSocketClientNode {
	return &WebSocketClientNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}
