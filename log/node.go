// Package log contains the log node implementation.
package log

import (
	"context"
	_ "embed"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(LogConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewLogNode(n, c), nil
}

// init registers the log node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "log",
			Name:        "Log Message",
			Description: "Logs a message to the console",
			Icon:        "terminal",
			Category:    "Debug",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the log node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"message": map[string]string{
			"type":        "string",
			"title":       "Log Message",
			"description": "The message to log",
		},
		"level": map[string]interface{}{
			"type":    "string",
			"title":   "Log Level",
			"enum":    []string{"info", "warn", "error", "debug"},
			"default": "info",
		},
	},
	"required": []string{"message"},
}

// LogConfig holds configuration for the log node
type LogConfig struct {
	Message string `json:"message"`
	Level   string `json:"level"`
}

// LogNode passes the payload through; its configured message is emitted
// into the workflow event log by the host (debug panel equivalent)
type LogNode struct {
	*nodebase.BaseNode
	config *LogConfig
}

// NewLogNode creates a new log node
func NewLogNode(node nodestore.NodeInterface, config *LogConfig) *LogNode {
	return &LogNode{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
	}
}

// Execute passes the input payload through unchanged
func (n *LogNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	return input, nil
}

// PostRunEvents implements RunEventer: the log node emits
// its configured message (at the configured level) into the workflow
// event log so it shows up in the debug panel.
func (n *LogNode) PostRunEvents(node nodestore.NodeInterface, out nodestore.MessageInterface) []nodebase.RunEvent {
	level, _ := node.GetData()["level"].(string)
	if level == "" {
		level = "info"
	}
	message, _ := node.GetData()["message"].(string)
	return []nodebase.RunEvent{{Level: level, Message: message}}
}
