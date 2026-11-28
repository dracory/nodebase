// Package httpresponse contains the httpResponse node implementation.
package httpresponse

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(HTTPResponseConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewHTTPResponse(n, c, e), nil
}

// init registers the httpResponse node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "httpResponse",
			Name:        "HTTP Response",
			Description: "Answers the request that triggered the flow",
			Icon:        "reply",
			Category:    "Actions",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the httpResponse node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"statusCode": map[string]interface{}{
			"type":        "integer",
			"title":       "Status Code",
			"description": "HTTP status code (per-message statusCode/headers in the payload take precedence)",
			"default":     200,
		},
		"headers": map[string]interface{}{
			"type":        "object",
			"title":       "Headers",
			"format":      "keyvalue",
			"description": "Extra response headers; a headers object in the message merges on top",
			"additionalProperties": map[string]string{
				"type": "string",
			},
		},
	},
}

// HTTPResponseConfig holds configuration for the HTTP response node
type HTTPResponseConfig struct {
	// StatusCode is the HTTP status sent with the reply (default 200).
	// Overridden per message by Metadata["statusCode"] or
	// Data["statusCode"] — Node-RED's msg.statusCode equivalent.
	StatusCode int `json:"statusCode"`
	// Headers are extra response headers. Per-message
	// Metadata["headers"] / Data["headers"] maps are merged on top —
	// Node-RED's msg.headers equivalent.
	Headers map[string]string `json:"headers,omitempty"`
}

// HTTPResponse answers the HTTP request that injected the current message.
// It correlates via the msgId in payload.Metadata, set by httpTrigger.
// Response status and headers travel back through the message metadata
// so the trigger handler can write them.
type HTTPResponse struct {
	*nodebase.BaseNode
	config *HTTPResponseConfig
	host   nodebase.NodeHost
}

// NewHTTPResponse creates a new HTTP response node
func NewHTTPResponse(node nodestore.NodeInterface, config *HTTPResponseConfig, host nodebase.NodeHost) *HTTPResponse {
	return &HTTPResponse{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
		host:     host,
	}
}

// Execute resolves the pending request associated with the message
func (n *HTTPResponse) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	msgID, _ := input.GetMetadata()["msgId"].(string)
	if msgID == "" {
		return nil, fmt.Errorf("httpResponse: message has no msgId (not triggered by an HTTP request)")
	}

	// Resolve effective status code: message override > config > 200
	statusCode := n.config.StatusCode
	if data, ok := input.GetPayload().(map[string]interface{}); ok {
		if v, ok := data["statusCode"].(float64); ok {
			statusCode = int(v)
		}
	}
	if v, ok := input.GetMetadata()["statusCode"].(float64); ok {
		statusCode = int(v)
	}
	if statusCode == 0 {
		statusCode = 200
	}

	// Resolve effective headers: config < message override
	headers := make(map[string]string, len(n.config.Headers)+2)
	for k, v := range n.config.Headers {
		headers[k] = v
	}
	mergeHeaders := func(m map[string]interface{}) {
		for k, v := range m {
			if s, ok := v.(string); ok {
				headers[k] = s
			}
		}
	}
	if data, ok := input.GetPayload().(map[string]interface{}); ok {
		if h, ok := data["headers"].(map[string]interface{}); ok {
			mergeHeaders(h)
		}
	}
	if h, ok := input.GetMetadata()["headers"].(map[string]interface{}); ok {
		mergeHeaders(h)
	}

	if input.GetMetadata() == nil {
		input.SetMetadata(map[string]interface{}{})
	}
	input.GetMetadata()["statusCode"] = statusCode
	input.GetMetadata()["responseHeaders"] = headers

	if err := n.host.ResolveResponse(msgID, input); err != nil {
		return nil, err
	}
	return input, nil
}
