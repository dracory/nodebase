// Package httptrigger contains the httpTrigger node implementation.
package httptrigger

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(HTTPTriggerConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	// Node-RED's "http in" node stores the route in `url`.
	if c.Path == "" {
		c.Path, _ = n.GetData()["url"].(string)
	}
	return NewHTTPTrigger(n, c, e), nil
}

// init registers the httpTrigger node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "httpTrigger",
			Name:        "HTTP Trigger",
			Description: "Starts a workflow with an HTTP request",
			Icon:        "bolt",
			Category:    "Triggers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the httpTrigger node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"method": map[string]interface{}{
			"type":        "string",
			"enum":        []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"},
			"default":     "GET",
			"description": "HTTP method the route answers (case-insensitive — Node-RED stores lowercase)",
		},
		"listener": map[string]interface{}{
			"type":    "string",
			"title":   "Listener",
			"enum":    []string{"own", "main"},
			"default": "own",
			"description": "own: bind a private listener on Port. main: serve Path on the admin " +
				"server's port (Node-RED style) — behind admin auth when configured; Port is ignored",
		},
		"path": map[string]interface{}{
			"type":        "string",
			"default":     "/trigger",
			"description": "URL path (\":\" segments become route params). listener \"main\": avoid /api, /editor, /dashboard and other admin prefixes — they are reserved",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"default":     8081,
			"description": "Port for the own listener (must differ from the admin port; ignored when listener is \"main\")",
		},
		"responseTimeout": map[string]interface{}{
			"type":        "number",
			"title":       "Response Timeout (s)",
			"description": "Seconds to wait for a downstream httpResponse before a 504 (0 = 15s default)",
			"default":     0,
		},
		// swaggerDoc / cors exist on Node-RED exports; tolerated so
		// imported flows deploy unchanged.
		"swaggerDoc": map[string]interface{}{
			"type":        "object",
			"title":       "Swagger Doc",
			"description": "Accepted for Node-RED compatibility; currently ignored at runtime",
		},
		"cors": map[string]interface{}{
			"title":       "CORS",
			"description": "Accepted for Node-RED compatibility; currently ignored at runtime",
		},
	},
	"required": []string{"path"},
}

// HTTPTriggerConfig holds configuration for the HTTP trigger node
type HTTPTriggerConfig struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Port   int    `json:"port"`
	// Listener selects where the route is served: "own" (default)
	// binds a private TCP listener on Port; "main" mounts the route
	// on the host's shared HTTP server via host.SharedHTTPRouter() — the
	// Node-RED model (declare a path, serve on the admin port,
	// behind the admin middleware).
	Listener string `json:"listener"`
	// ResponseTimeout is how many seconds a request waits for a
	// downstream httpResponse before a 504 is returned
	// (0 = defaultResponseTimeout).
	ResponseTimeout float64 `json:"responseTimeout"`
	// SwaggerDoc and Cors appear on Node-RED exports; decoded and
	// preserved for round-trip fidelity but ignored at runtime.
	SwaggerDoc json.RawMessage `json:"swaggerDoc,omitempty"`
	Cors       json.RawMessage `json:"cors,omitempty"`
}

// HTTPTrigger is a node that triggers a workflow via HTTP.
// Each incoming request is injected into the running workflow as a
// payload; the request is answered by a downstream httpResponse node,
// or with a 504 error if the flow produces no response in time.
type HTTPTrigger struct {
	*nodebase.BaseNode
	config     *HTTPTriggerConfig
	serverMu   sync.Mutex
	server     *http.Server
	host       nodebase.NodeHost
	workflowID string
	// params lists the ":name" route parameters converted into
	// ServeMux "{name}" wildcards — extracted per request into
	// Data["params"].
	params []string
	// mounted is the method-qualified pattern mounted on the shared
	// router in listener "main" mode ("" = not mounted); router is
	// the router it was mounted on, captured so Stop unregisters on
	// the same instance even if the host field is swapped.
	mounted string
	router  nodebase.HTTPRouter
}

// defaultResponseTimeout is how long a trigger waits for the flow to
// produce an httpResponse before falling back to a 504 (Node-RED waits
// indefinitely; the finite default is a deliberate deviation).
const defaultResponseTimeout = 15 * time.Second

// maxTriggerBodyBytes caps request bodies decoded by the trigger
// (Node-RED's body-parser default is ~100kb; 1 MiB leaves headroom).
const maxTriggerBodyBytes = 1 << 20

// NewHTTPTrigger creates a new HTTP trigger node
func NewHTTPTrigger(node nodestore.NodeInterface, config *HTTPTriggerConfig, host nodebase.NodeHost) *HTTPTrigger {
	return &HTTPTrigger{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute starts the HTTP server to listen for incoming requests
func (n *HTTPTrigger) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	// Everything fallible or panicky runs before the lock so the
	// serverMu critical section is a single check-and-assign with no
	// way to leak it.
	if err := n.Validate(); err != nil {
		return nil, err
	}

	n.serverMu.Lock()
	running := n.server != nil || n.mounted != ""
	n.serverMu.Unlock()
	if running {
		return nil, fmt.Errorf("HTTP server already running")
	}

	// Validate already rejected bad patterns; the values of ":name"
	// route parameters land in Data["params"].
	pattern, params := n.routePattern()

	if n.config.Listener == "main" {
		return n.mountShared(pattern, params)
	}

	handler := http.NewServeMux()
	handler.HandleFunc(pattern, n.handleRequest)

	port := n.config.Port
	if port <= 0 {
		port = 8081 // must not collide with the admin server on :8080
	}

	// Bind synchronously so a port conflict surfaces as a node error
	// instead of a console message after the run has "succeeded"
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("cannot bind trigger listener on :%d: %w", port, err)
	}

	n.serverMu.Lock()
	defer n.serverMu.Unlock()
	if n.server != nil {
		listener.Close()
		return nil, fmt.Errorf("HTTP server already running")
	}
	n.params = params
	n.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: handler,
		// DoS guards: a request can stall a flow for up to
		// responseTimeout, so WriteTimeout covers that plus slack.
		ReadHeaderTimeout: nodebase.ReadHeaderTimeout,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      n.responseTimeout() + 15*time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	// Capture the server locally — Stop may nil n.server before this
	// goroutine evaluates it, which would panic on the dereference.
	srv := n.server
	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	// Return initial payload
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening"}).SetTimestamp(time.Now()), nil
}

// mountShared registers the trigger's route on the host's shared
// HTTPRouter (listener "main") instead of binding a private port.
// The pattern is method-qualified so the shared mux enforces the
// method (405 on mismatch), matching the own listener's manual check.
// A route conflict is an activation error — the second registrant's
// deploy fails, exactly like a port bind conflict in "own" mode.
func (n *HTTPTrigger) mountShared(pattern string, params []string) (nodestore.MessageInterface, error) {
	if n.host == nil || n.host.SharedHTTPRouter() == nil {
		return nil, fmt.Errorf("listener \"main\" requires a shared HTTP router on the host (host.SharedHTTPRouter()) — none is installed")
	}
	full := n.config.Method + " " + pattern
	if err := n.host.SharedHTTPRouter().Handle(full, http.HandlerFunc(n.handleRequest)); err != nil {
		return nil, fmt.Errorf("cannot mount trigger route %q on the shared listener: %w", full, err)
	}
	n.serverMu.Lock()
	defer n.serverMu.Unlock()
	if n.mounted != "" {
		n.host.SharedHTTPRouter().Unregister(full)
		return nil, fmt.Errorf("HTTP trigger already mounted")
	}
	n.params = params
	n.router = n.host.SharedHTTPRouter()
	n.mounted = full
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening", "listener": "main"}).SetTimestamp(time.Now()), nil
}

// handleRequest processes incoming HTTP requests
func (n *HTTPTrigger) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Only handle the configured HTTP method
	if r.Method != n.config.Method {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body if present — bounded so an unauthenticated
	// client cannot exhaust memory with an unbounded upload. The
	// decoded shape depends on Content-Type (see parseRequestBody).
	var requestData interface{}
	if r.Body != nil {
		var ok bool
		requestData, ok = n.parseRequestBody(w, r)
		if !ok {
			return
		}
	}

	// Create the message in Node-RED's http-in shape. GET carries the
	// query object in msg.payload; body-capable methods carry the parsed
	// request body. The request object itself is msg.req (represented in
	// metadata because the message envelope separates payload/metadata).
	query := queryObject(r.URL.Query())
	headers := headerObject(r.Header)
	params := map[string]string{}
	for _, p := range n.params {
		params[p] = r.PathValue(p)
	}

	var messagePayload interface{}
	switch n.config.Method {
	case http.MethodGet:
		messagePayload = query
	case http.MethodPost, http.MethodDelete, http.MethodPut, http.MethodOptions, http.MethodPatch:
		messagePayload = requestData
		if messagePayload == nil {
			messagePayload = "" // Node-RED's raw body parser initializes req.body to an empty string
		}
	}
	payload := nodestore.NewMessage().SetPayload(messagePayload).SetMetadata(make(map[string]interface{})).SetTimestamp(time.Now())

	// Add request metadata — the flat keys are kept for existing
	// flows; "req" is the Node-RED msg.req equivalent. Unlike Go's
	// url.Values/http.Header maps, Node-RED query and header values are
	// lower-case single strings when the key occurs once.
	payload.GetMetadata()["method"] = r.Method
	payload.GetMetadata()["path"] = r.URL.Path
	payload.GetMetadata()["query"] = r.URL.Query()
	payload.GetMetadata()["headers"] = r.Header
	payload.GetMetadata()["remoteAddr"] = r.RemoteAddr
	payload.GetMetadata()["req"] = map[string]interface{}{
		"method":     r.Method,
		"path":       r.URL.Path,
		"query":      query,
		"params":     params,
		"headers":    headers,
		"cookies":    requestCookies(r),
		"remoteAddr": r.RemoteAddr,
		"body":       requestData,
	}

	// Inject the payload into the running workflow; a downstream
	// httpResponse node answers the request via its message channel
	msgID, ch, err := n.host.InjectMessage(n.workflowID, n.GetID(), payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	select {
	case response := <-ch:
		// httpResponse marks up status + headers via message metadata;
		// Node-RED's msg.statusCode / msg.headers equivalent
		if headers, ok := response.GetMetadata()["responseHeaders"].(map[string]string); ok {
			for k, v := range headers {
				w.Header().Set(k, v)
			}
		}
		statusCode := 200
		if sc, ok := response.GetMetadata()["statusCode"].(int); ok && sc > 0 {
			statusCode = sc
		}
		// Express res.send semantics: strings are HTML unless the flow
		// explicitly set a content type; buffers are raw binary; other
		// values are JSON.
		switch response.GetPayload().(type) {
		case string:
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
			}
		case []byte:
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", "application/octet-stream")
			}
		default:
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", "application/json")
			}
		}
		w.WriteHeader(statusCode)
		// Strings and bytes go out raw; anything else is JSON-encoded.
		switch body := response.GetPayload().(type) {
		case string:
			w.Write([]byte(body))
		case []byte:
			w.Write(body)
		default:
			json.NewEncoder(w).Encode(response.GetPayload())
		}
	case <-time.After(n.responseTimeout()):
		// No httpResponse node in the flow (or the flow stalled)
		n.host.DiscardPending(msgID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "flow produced no response within the response timeout — is an httpResponse node wired downstream?",
		})
	case <-r.Context().Done():
		n.host.DiscardPending(msgID)
	}
}

// parseRequestBody decodes the request body by Content-Type, Node-RED
// http-in style:
//   - application/json (or any */*+json) → decoded JSON value
//   - application/x-www-form-urlencoded → map (repeated keys become slices)
//   - text/*, XML, missing content type, and UTF-8 application types → string
//   - binary application types and non-application/non-text types → raw bytes ([]byte)
//
// The body is capped at maxTriggerBodyBytes; on failure it writes the
// HTTP error itself and returns ok=false so the caller just returns.
func (n *HTTPTrigger) parseRequestBody(w http.ResponseWriter, r *http.Request) (data interface{}, ok bool) {
	if r.ContentLength > maxTriggerBodyBytes {
		http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTriggerBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) || strings.Contains(err.Error(), "request body too large") {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
		}
		return nil, false
	}
	if len(body) == 0 {
		return nil, true
	}

	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	switch {
	case mediaType == "application/json" || strings.HasSuffix(mediaType, "+json"):
		var decoded interface{}
		if err := json.Unmarshal(body, &decoded); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return nil, false
		}
		return decoded, true
	case mediaType == "application/x-www-form-urlencoded":
		values, err := url.ParseQuery(string(body))
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return nil, false
		}
		form := make(map[string]interface{}, len(values))
		for k, v := range values {
			if len(v) == 1 {
				form[k] = v[0]
			} else {
				form[k] = v
			}
		}
		return form, true
	case mediaType == "":
		// Node-RED's raw body parser defaults to text when no content
		// type is present.
		return string(body), true
	case strings.HasPrefix(mediaType, "text/") || mediaType == "application/xml" || strings.HasSuffix(mediaType, "+xml"):
		return string(body), true
	case strings.HasPrefix(mediaType, "application/"):
		subtype := strings.TrimPrefix(mediaType, "application/")
		if subtype == "octet-stream" || subtype == "cbor" || subtype == "x-protobuf" {
			return body, true
		}
		if utf8.Valid(body) {
			return string(body), true
		}
		return body, true
	default:
		return body, true
	}
}

// queryObject converts url.Values to the Express/Node-RED req.query
// shape: a single occurrence is a string, repeated keys stay arrays.
func queryObject(values url.Values) map[string]interface{} {
	out := make(map[string]interface{}, len(values))
	for k, v := range values {
		if len(v) == 1 {
			out[k] = v[0]
		} else {
			out[k] = append([]string(nil), v...)
		}
	}
	return out
}

// headerObject converts Go's canonical-cased http.Header to the
// lower-case Node.js/Express req.headers shape exposed by Node-RED.
func headerObject(headers http.Header) map[string]interface{} {
	out := make(map[string]interface{}, len(headers))
	for k, v := range headers {
		key := strings.ToLower(k)
		if len(v) == 1 {
			out[key] = v[0]
		} else {
			out[key] = append([]string(nil), v...)
		}
	}
	return out
}

// requestCookies flattens the request's Cookie header into a
// name→value map (Node-RED msg.req.cookies equivalent).
func requestCookies(r *http.Request) map[string]string {
	cookies := r.Cookies()
	if len(cookies) == 0 {
		return nil
	}
	out := make(map[string]string, len(cookies))
	for _, c := range cookies {
		out[c.Name] = c.Value
	}
	return out
}

// responseTimeout returns the configured response wait (default 15s).
func (n *HTTPTrigger) responseTimeout() time.Duration {
	if n.config.ResponseTimeout > 0 {
		return time.Duration(n.config.ResponseTimeout * float64(time.Second))
	}
	return defaultResponseTimeout
}

// Validate checks if the node is properly configured
func (n *HTTPTrigger) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}

	if n.config.Path == "" {
		return fmt.Errorf("path is required")
	}

	if n.config.Method == "" {
		n.config.Method = "GET" // Default to GET if not specified
	}
	// Node-RED stores the method lowercase ("get"); normalize so
	// imported flows match the configured route.
	n.config.Method = strings.ToUpper(n.config.Method)
	switch n.config.Method {
	case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS":
	default:
		return fmt.Errorf("invalid method %q", n.config.Method)
	}

	switch n.config.Listener {
	case "", "own", "main":
	default:
		return fmt.Errorf("invalid listener %q — expected \"own\" or \"main\"", n.config.Listener)
	}

	// Reject paths whose mux pattern is invalid (e.g. duplicate ":name"
	// parameters) — ServeMux.HandleFunc panics on them, which would
	// otherwise surface as an Execute panic instead of a deploy error.
	pattern, _ := n.routePattern()
	var patErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				patErr = fmt.Errorf("invalid path %q: %v", n.config.Path, r)
			}
		}()
		probe := http.NewServeMux()
		probe.HandleFunc(pattern, func(http.ResponseWriter, *http.Request) {})
	}()
	return patErr
}

// routePattern returns the ServeMux pattern and parameter names for the
// configured path. Execute and Validate share it so the deploy-time
// check probes exactly the pattern that will be served. net/http
// patterns must start with a slash; an empty or malformed path would
// panic in HandleFunc. ":name" segments become Go 1.22 "{name}" wildcards
// (Node-RED route parameters).
func (n *HTTPTrigger) routePattern() (string, []string) {
	path := n.config.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return nodebase.MuxPattern(path)
}

// Stop shuts down the HTTP server
func (n *HTTPTrigger) Stop() error {
	// Take the server under the lock and clear it first — concurrent
	// Stop calls (StopWorkflow vs a mid-activation cancel) must be
	// idempotent and never race on n.server.
	n.serverMu.Lock()
	srv := n.server
	n.server = nil
	mounted := n.mounted
	n.mounted = ""
	router := n.router
	n.router = nil
	n.serverMu.Unlock()
	if mounted != "" && router != nil {
		router.Unregister(mounted)
	}
	if srv == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("error shutting down HTTP server: %w", err)
	}
	return nil
}
