// Package httprequest contains the httpRequest node implementation.
package httprequest

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

//go:embed editor.html
var editorHTML string

// maxResponseBodyBytes caps how much of a response body is read into
// memory (50 MiB) — see the io.LimitReader read in Execute.
const maxResponseBodyBytes = 50 << 20

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(HTTPRequestConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewHTTPRequest(n, c, e), nil
}

// init registers the httpRequest node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "httpRequest",
			Name:        "HTTP Request",
			Description: "Makes an HTTP request to a specified URL",
			Icon:        "globe",
			Category:    "Actions",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the httpRequest node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"url": map[string]string{
			"type":        "string",
			"title":       "URL",
			"description": "The URL to make the request to",
		},
		"method": map[string]interface{}{
			"type":    "string",
			"title":   "HTTP Method",
			"enum":    []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
			"default": "GET",
		},
		"headers": map[string]interface{}{
			"type":        "object",
			"title":       "Headers",
			"format":      "keyvalue",
			"description": "Extra request headers sent with every request",
			"additionalProperties": map[string]string{
				"type": "string",
			},
		},
		"body": map[string]string{
			"type":        "string",
			"title":       "Request Body",
			"description": "The request body (for POST/PUT/PATCH requests)",
		},
		"username": map[string]interface{}{
			"type":        "string",
			"format":      "credential",
			"title":       "Username",
			"description": "Optional basic-auth username (credential: masked, never exported)",
		},
		"password": map[string]interface{}{
			"type":        "string",
			"format":      "credential",
			"title":       "Password",
			"description": "Optional basic-auth password (credential: masked, never exported)",
		},
		"errorCatch": nodebase.ErrorCatchSchemaField(),
	},
	"required": []string{"url", "method"},
}

// HTTPRequestConfig holds configuration for the HTTP request node
type HTTPRequestConfig struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    interface{}       `json:"body,omitempty"`
	// Username/Password set an HTTP Basic auth credential on the
	// request. They are credential-typed fields: stored outside the
	// exported flow document and merged back in at deploy time.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// HTTPRequest is a node that makes HTTP requests
// It implements the NodeExecutor interface
type HTTPRequest struct {
	*nodebase.BaseNode
	config *HTTPRequestConfig
	host   nodebase.NodeHost
}

// NewHTTPRequest creates a new HTTP request node. host is variadic
// for backward compatibility (older callers passed none); supply the
// NodeHost so the optional OutboundURLAllowed policy is honoured.
func NewHTTPRequest(node nodestore.NodeInterface, config *HTTPRequestConfig, hosts ...nodebase.NodeHost) *HTTPRequest {
	var host nodebase.NodeHost
	if len(hosts) > 0 {
		host = hosts[0]
	}
	return &HTTPRequest{
		BaseNode: nodebase.NewBaseNode(node),
		config:   config,
		host:     host,
	}
}

// Execute makes the HTTP request and returns the response
func (n *HTTPRequest) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	// Parse URL from config or input. A message-supplied "url" overrides
	// an empty config URL (Node-RED msg.url parity) — note this means a
	// flow fed by an httpTrigger lets the caller pick the destination;
	// only http/https targets are allowed.
	rawURL := n.config.URL
	if rawURL == "" {
		// Try to get URL from input
		if inputData, ok := input.GetPayload().(map[string]interface{}); ok {
			if urlVal, ok := inputData["url"].(string); ok && urlVal != "" {
				rawURL = urlVal
			} else {
				return nil, fmt.Errorf("URL is required")
			}
		} else {
			return nil, fmt.Errorf("invalid input data format")
		}
	}
	parsedURL, err := url.Parse(rawURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return nil, fmt.Errorf("unsupported URL %q: only http:// and https:// targets are allowed", rawURL)
	}
	// Optional host SSRF policy: hosts embedding nodebase can veto
	// outbound targets (internal ranges, metadata endpoints) by
	// implementing OutboundURLAllowed. Absent = allow (v1 default).
	if p, ok := n.host.(interface{ OutboundURLAllowed(rawURL string) bool }); ok && !p.OutboundURLAllowed(parsedURL.String()) {
		return nil, fmt.Errorf("URL %q rejected by the host outbound policy", rawURL)
	}

	// Parse method from config or input, default to GET
	method := n.config.Method
	if method == "" {
		if inputData, ok := input.GetPayload().(map[string]interface{}); ok {
			if methodVal, ok := inputData["method"].(string); ok && methodVal != "" {
				method = methodVal
			} else {
				method = http.MethodGet
			}
		} else {
			method = http.MethodGet
		}
	}

	// Prepare request body. GET and HEAD requests carry no body
	// (Node-RED parity). Strings and []byte go out raw; anything else is
	// JSON-encoded (and only then gets a JSON Content-Type).
	var body io.Reader
	bodyIsJSON := false
	if method != http.MethodGet && method != http.MethodHead {
		var bodyData interface{}
		if n.config.Body != nil {
			bodyData = n.config.Body
		} else {
			bodyData = input.GetPayload()
		}
		switch v := bodyData.(type) {
		case nil:
		case string:
			body = strings.NewReader(v)
		case []byte:
			body = bytes.NewReader(v)
		default:
			jsonData, err := nodebase.PayloadBytes(v)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal request body: %w", err)
			}
			body = bytes.NewBuffer(jsonData)
			bodyIsJSON = true
		}
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, method, parsedURL.String(), body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	headers := make(map[string]string)
	if n.config.Headers != nil {
		for k, v := range n.config.Headers {
			headers[k] = v
		}
	}

	// Add a JSON Content-Type only when the body was JSON-encoded and no
	// Content-Type was configured
	if bodyIsJSON {
		if _, ok := headers["Content-Type"]; !ok {
			headers["Content-Type"] = "application/json"
		}
	}

	// Set request headers
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// Basic auth (Node-RED "use authentication" parity) — only when a
	// username is configured; an explicit Authorization header wins.
	if n.config.Username != "" {
		if _, ok := headers["Authorization"]; !ok {
			req.SetBasicAuth(n.config.Username, n.config.Password)
		}
	}

	// Make the request. Redirects get the same scheme gate as the
	// initial URL — otherwise a permitted URL could bounce the request
	// to a non-HTTP scheme or an internal address.
	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("redirect to unsupported scheme %q refused", req.URL.Scheme)
			}
			// Redirects re-check the host policy — a permitted URL must
			// not bounce the request to a denied target.
			if p, ok := n.host.(interface{ OutboundURLAllowed(rawURL string) bool }); ok && !p.OutboundURLAllowed(req.URL.String()) {
				return fmt.Errorf("redirect to %q rejected by the host outbound policy", req.URL.Host)
			}
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			return nil
		},
	}

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("request failed: empty response")
	}
	defer func() { _ = resp.Body.Close() }()

	// Read response body, capped: an upstream can stream unbounded
	// data otherwise and a fast steady stream can exhaust memory
	// well inside the 30s client timeout. The payload reports
	// truncation via "bodyTruncated".
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	truncated := int64(len(respBody)) > maxResponseBodyBytes
	if truncated {
		respBody = respBody[:maxResponseBodyBytes]
	}

	// Parse response body if it's JSON. The media type must be parsed —
	// parameters like "; charset=utf-8" are common — and typed JSON
	// variants (application/*+json) count as JSON too.
	var responseData interface{}
	mediaType, _, mtErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	isJSON := mtErr == nil && (mediaType == "application/json" ||
		(strings.HasPrefix(mediaType, "application/") && strings.HasSuffix(mediaType, "+json")))
	if isJSON {
		if err := json.Unmarshal(respBody, &responseData); err != nil {
			// If not valid JSON, return as string
			responseData = string(respBody)
		}
	} else {
		responseData = string(respBody)
	}

	// Create response payload
	response := map[string]interface{}{
		"statusCode":    resp.StatusCode,
		"bodyTruncated": truncated,
		"status":        resp.Status,
		"headers":       resp.Header,
		"body":          responseData,
		"duration":      time.Since(startTime).Milliseconds(),
	}

	// Return the response as the output payload
	return nodestore.NewMessage().SetPayload(response).SetMetadata(input.GetMetadata()).SetTimestamp(time.Now()), nil
}

// Validate checks if the node is properly configured
func (n *HTTPRequest) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}

	if n.config.URL == "" {
		// URL can be provided at runtime, so this is not a hard error
		// Just log a warning
		fmt.Println("Warning: URL not provided in config, must be provided at runtime")
	}

	// Set default method if not provided
	if n.config.Method == "" {
		n.config.Method = http.MethodGet
	}

	return nil
}
