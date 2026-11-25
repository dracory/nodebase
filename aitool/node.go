// Package aitool contains the ai-tool node implementation: the entry
// point of a flow exposed to an ai-agent as a callable tool. The node
// declares the tool's name, description and parameter schema; the
// agent injects a call message (arguments in the payload, correlation
// via the stamped message id) which travels the wired flow and is
// answered by an ai-tool-return node.
package aitool

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dracory/llm"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// Type is the registered node type id.
const Type = "ai-tool"

// MetaKey is the message-metadata key under which the tool-call
// context (id, name, arguments) travels the tool flow.
const MetaKey = "aiToolCall"

// MsgIDKey is the metadata key the host stamps onto injected messages;
// ai-tool-return reads it to resolve the pending call (same
// convention as linkOut's return mode).
const MsgIDKey = "msgId"

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(Config)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewToolNode(n, c, e), nil
}

// init registers the ai-tool node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        Type,
			Name:        "AI Tool",
			Description: "Entry point of a flow exposed to an ai-agent as a callable tool",
			Icon:        "wrench",
			Category:    "AI",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the ai-tool node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"toolName": map[string]interface{}{
			"type":        "string",
			"title":       "Tool Name",
			"description": "Function name the model calls (letters, digits, _ and -); defaults to the node id",
		},
		"description": map[string]interface{}{
			"type":        "string",
			"title":       "Description",
			"description": "Tells the model when to use this tool",
		},
		"parameters": map[string]interface{}{
			"type":                 "object",
			"title":                "Parameters",
			"description":          "JSON Schema object describing the tool arguments; empty sends an open object schema",
			"additionalProperties": true,
		},
	},
}

// Config holds the ai-tool configuration.
type Config struct {
	// ToolName is the function identifier the model calls; empty
	// defaults to the node id.
	ToolName string `json:"toolName"`
	// Description tells the model when to use the tool.
	Description string `json:"description"`
	// Parameters is the JSON Schema object describing the arguments.
	Parameters map[string]interface{} `json:"parameters"`
}

// ToolNode is the ai-tool executor — a pass-through entry that stamps
// the tool-call context into message metadata.
type ToolNode struct {
	*nodebase.BaseNode
	config     *Config
	host       nodebase.NodeHost
	workflowID string
}

// NewToolNode creates a new ai-tool node.
func NewToolNode(node nodestore.NodeInterface, config *Config, host nodebase.NodeHost) *ToolNode {
	return &ToolNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute stamps the tool-call metadata and passes the call message
// down the tool flow. The agent's injected message already carries
// the arguments as payload plus the stamped pending message id;
// ai-tool-return answers it downstream.
func (n *ToolNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if input.GetMetadata() == nil {
		input.SetMetadata(map[string]interface{}{})
	}
	call, _ := input.GetMetadata()[MetaKey].(map[string]interface{})
	if call == nil {
		call = map[string]interface{}{}
	}
	if _, ok := call["name"]; !ok {
		call["name"] = n.Name()
	}
	call["toolNodeId"] = n.GetID()
	input.GetMetadata()[MetaKey] = call
	if n.host != nil {
		n.host.EmitEvent(n.workflowID, n.GetID(), nodebase.NodeName(n.NodeInterface), "info",
			fmt.Sprintf("tool %q invoked", n.Name()))
	}
	return input, nil
}

// Name resolves the effective tool name (configured name, else node id).
func (n *ToolNode) Name() string {
	return NameFor(n.NodeInterface, n.config)
}

// Validate requires nothing beyond structural sanity — the tool name
// defaults to the node id when empty.
func (n *ToolNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if name := n.Name(); !validToolName(name) {
		return fmt.Errorf("ai-tool: invalid tool name %q", name)
	}
	return nil
}

// NameFor resolves the effective tool name off a stored node
// definition: the configured name, else the node id.
func NameFor(node nodestore.NodeInterface, c *Config) string {
	if c != nil && c.ToolName != "" {
		return c.ToolName
	}
	return node.GetID()
}

// validToolName reports whether s is usable as an LLM function name
// (providers accept letters, digits, underscore and dash).
func validToolName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

// ConfigForNode decodes the ai-tool config off a stored node
// definition — the path ai-agent takes after LookupNode.
func ConfigForNode(node nodestore.NodeInterface) (*Config, error) {
	if node == nil || node.GetType() != Type {
		return nil, fmt.Errorf("node is not an %s node", Type)
	}
	c := new(Config)
	if err := nodebase.DecodeConfig(node, c); err != nil {
		return nil, err
	}
	return c, nil
}

// ToolForNode builds the llm.Tool declaration an ai-agent advertises
// for this ai-tool node.
func ToolForNode(node nodestore.NodeInterface) (llm.Tool, error) {
	c, err := ConfigForNode(node)
	if err != nil {
		return llm.Tool{}, err
	}
	return llm.Tool{
		Name:        NameFor(node, c),
		Description: c.Description,
		Parameters:  c.Parameters,
	}, nil
}

// ParseRef splits a tool reference of the form "workflowID/nodeID" or
// a bare "nodeID" (same workflow as the agent). Whitespace is trimmed;
// an empty segment is an error.
func ParseRef(ref, defaultWorkflowID string) (workflowID, nodeID string, err error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", "", fmt.Errorf("empty tool reference")
	}
	if i := strings.Index(ref, "/"); i >= 0 {
		workflowID, nodeID = ref[:i], ref[i+1:]
		if workflowID == "" || nodeID == "" || strings.Contains(nodeID, "/") {
			return "", "", fmt.Errorf("invalid tool reference %q", ref)
		}
		return workflowID, nodeID, nil
	}
	return defaultWorkflowID, ref, nil
}

// DecodeArguments decodes a tool call's raw JSON argument string into
// a message payload: objects decode to map[string]interface{}, other
// JSON values to their natural types, and non-JSON input passes
// through as a string.
func DecodeArguments(raw string) interface{} {
	if raw == "" {
		return map[string]interface{}{}
	}
	var v interface{}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return raw
	}
	return v
}
