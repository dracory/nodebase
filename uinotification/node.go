// Package uinotification contains the uiNotification dashboard
// widget: a toast on the dashboard page driven by the incoming
// message (text/level). Level defaults to "info"; an explicit
// ui.level or a payload map's "level" key wins.
package uinotification

import (
	"context"
	_ "embed"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	return NewUINotificationNode(n, e), nil
}

// init registers the uiNotification node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "uiNotification",
			Name:        "UI Notification",
			Description: "Dashboard toast from an incoming message (text/level)",
			Icon:        "bell",
			Category:    "Dashboard",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the uiNotification node.
var schema = map[string]interface{}{
	"type":       "object",
	"properties": nodebase.UIWithLayout(nil),
}

// UINotificationNode pushes {"text": ..., "level": ...} to the
// dashboard sink. The client toasts only on live updates — manifest
// replay does not re-fire stale notifications.
type UINotificationNode struct {
	*nodebase.UIWidgetNode
}

// NewUINotificationNode creates a new uiNotification widget node.
func NewUINotificationNode(node nodestore.NodeInterface, host nodebase.NodeHost) *UINotificationNode {
	return &UINotificationNode{UIWidgetNode: nodebase.NewUIWidgetNode(node, host)}
}

// Execute pushes {"text": ..., "level": ...} to the hub.
func (n *UINotificationNode) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	data := in.GetPayload()
	state := map[string]interface{}{
		"text":  nodebase.UIFormatValue(nodebase.UIPayloadValue(data)),
		"level": "info",
	}
	if m, ok := nodebase.UIPayloadValue(data).(map[string]interface{}); ok {
		if l, ok := m["level"].(string); ok && l != "" {
			state["level"] = l
		}
		if t, ok := m["text"].(string); ok {
			state["text"] = t
		}
	}
	if ui := nodebase.UIOverrides(data); ui != nil {
		nodebase.UIApplyOverrides(state, ui)
	}
	n.Push(state)
	return in, nil
}
