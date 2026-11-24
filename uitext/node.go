// Package uitext contains the uiText dashboard widget: a label +
// formatted value readout. The executor computes the widget state and
// pushes it through the host's dashboard sink (see nodes/uishare.go);
// the message passes through unchanged downstream (Node-RED ui nodes
// are not sinks).
package uitext

import (
	"context"
	_ "embed"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	return NewUITextNode(n, e), nil
}

// init registers the uiText node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "uiText",
			Name:        "UI Text",
			Description: "Dashboard readout: label + formatted msg payload",
			Icon:        "font",
			Category:    "Dashboard",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the uiText node.
var schema = map[string]interface{}{
	"type":       "object",
	"properties": nodebase.UIWithLayout(nil),
}

// UITextNode pushes {"value": formatted, "label"?: override} to the
// dashboard sink.
type UITextNode struct {
	*nodebase.UIWidgetNode
}

// NewUITextNode creates a new uiText widget node.
func NewUITextNode(node nodestore.NodeInterface, host nodebase.NodeHost) *UITextNode {
	return &UITextNode{UIWidgetNode: nodebase.NewUIWidgetNode(node, host)}
}

// Execute pushes {"value": formatted, "label"?: override} to the hub.
// The message passes through unchanged downstream.
func (n *UITextNode) Execute(ctx context.Context, in nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	data := in.GetPayload()
	state := map[string]interface{}{
		"value": nodebase.UIFormatValue(nodebase.UIPayloadValue(data)),
		"label": "", // placeholder key so ui.label overrides may apply
	}
	if ui := nodebase.UIOverrides(data); ui != nil {
		nodebase.UIApplyOverrides(state, ui)
	}
	if state["label"] == "" {
		delete(state, "label")
	}
	n.Push(state)
	return in, nil
}
