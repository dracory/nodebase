// Package submitin contains the submitIn node implementation.
package submitin

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
	"strings"
	"sync"
	"time"
)

//go:embed editor.html
var editorHTML string

func factory(n nodestore.NodeInterface, e nodebase.NodeHost) (nodebase.NodeExecutor, error) {
	c := new(SubmitConfig)
	if err := nodebase.DecodeConfig(n, c); err != nil {
		return nil, err
	}
	return NewSubmitNode(n, c, e), nil
}

// init registers the submitIn node with the core set.
func init() {
	nodebase.MustRegisterNode(nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{
			Type:        "submitIn",
			Name:        "Submit",
			Description: "Receives uploaded files via a web submit point and stamps the submitting user into the message",
			Icon:        "upload",
			Category:    "Triggers",
			Set:         "core",
			Schema:      schema,
			EditorHTML:  editorHTML,
		},
		Factory: factory,
	})
}

// schema is the JSON schema the editor serves for the submitIn node.
var schema = map[string]interface{}{
	"type": "object",
	"properties": map[string]interface{}{
		"path": map[string]interface{}{
			"type":        "string",
			"title":       "Path",
			"default":     "/submit",
			"description": "URL path accepting uploads (POST). \":name\" segments become route params in msg.submit.params",
		},
		"listener": map[string]interface{}{
			"type":    "string",
			"title":   "Listener",
			"enum":    []string{"own", "main"},
			"default": "own",
			"description": "own: bind a private listener on Port. main: serve Path on the admin " +
				"server's port — behind admin auth when configured; Port is ignored",
		},
		"port": map[string]interface{}{
			"type":        "integer",
			"default":     8081,
			"description": "Port for the upload listener (must differ from the admin port; ignored when listener is \"main\")",
		},
		"maxSize": map[string]interface{}{
			"type":        "integer",
			"title":       "Max Upload Size (bytes)",
			"description": "Largest accepted upload; oversized requests are rejected with 413 (0 = 10 MiB default)",
			"default":     0,
		},
		"auth": map[string]interface{}{
			"type":    "string",
			"title":   "Authentication",
			"enum":    []string{"none", "admin"},
			"default": "none",
			"description": "admin: require HTTP Basic credentials from the admin auth file " +
				"(GOFLOW_AUTH_FILE); the authenticated username lands in msg.submit.submittedBy",
		},
	},
}

// SubmitConfig holds the node.Data fields of a submitIn node.
type SubmitConfig struct {
	// Path is the URL path the upload endpoint listens on
	// (default "/submit"; ":name" segments become route params).
	Path string `json:"path"`
	// Port for the upload listener (default 8081 — must differ from
	// the admin port, same model as httpTrigger).
	Port int `json:"port"`
	// Listener selects where the route is served: "own" (default)
	// binds a private TCP listener on Port; "main" mounts the route
	// on the host's shared HTTP server via host.SharedHTTPRouter(), behind
	// the admin middleware — same model as httpTrigger.
	Listener string `json:"listener"`
	// MaxSize caps the upload in bytes (0 = defaultMaxSubmitBytes).
	MaxSize int64 `json:"maxSize"`
	// Auth is "none" (default) or "admin" (require credentials from
	// the admin auth file; the username lands in submittedBy).
	Auth string `json:"auth"`
}

// defaultMaxSubmitBytes caps uploads when maxSize is unset: 10 MiB
// keeps payloads honest for the bytes-inline v1 (a payloadFile
// artifact reference can lift this later).
const defaultMaxSubmitBytes = 10 << 20

// SubmitNode is a node that receives uploaded files over HTTP and
// injects them into the running workflow with submit metadata.
type SubmitNode struct {
	*nodebase.BaseNode
	config     *SubmitConfig
	serverMu   sync.Mutex
	server     *http.Server
	host       nodebase.NodeHost
	workflowID string
	// params lists ":name" route parameters extracted per request.
	params []string
	// mounted/router track the shared-router registration in
	// listener "main" mode (same fields as HTTPTrigger).
	mounted string
	router  nodebase.HTTPRouter
}

// NewSubmitNode creates a new submitIn node executor.
func NewSubmitNode(node nodestore.NodeInterface, config *SubmitConfig, host nodebase.NodeHost) *SubmitNode {
	return &SubmitNode{
		BaseNode:   nodebase.NewBaseNode(node),
		config:     config,
		host:       host,
		workflowID: node.GetWorkflowID(),
	}
}

// Execute binds the upload listener and returns immediately; each
// accepted upload is injected downstream as it arrives.
func (n *SubmitNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	if err := n.Validate(); err != nil {
		return nil, err
	}

	n.serverMu.Lock()
	running := n.server != nil || n.mounted != ""
	n.serverMu.Unlock()
	if running {
		return nil, fmt.Errorf("submit listener already running")
	}

	pattern, params := n.routePattern()

	if n.config.Listener == "main" {
		return n.mountShared(pattern, params)
	}

	handler := http.NewServeMux()
	handler.HandleFunc(pattern, n.handleRequest)

	port := n.config.Port
	if port <= 0 {
		port = 8081
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("cannot bind submit listener on :%d: %w", port, err)
	}

	n.serverMu.Lock()
	defer n.serverMu.Unlock()
	if n.server != nil {
		listener.Close()
		return nil, fmt.Errorf("submit listener already running")
	}
	n.params = params
	n.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadHeaderTimeout: nodebase.ReadHeaderTimeout,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	srv := n.server
	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("submit listener error: %v\n", err)
		}
	}()

	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening"}).SetTimestamp(time.Now()), nil
}

// mountShared registers the upload route on the host's shared
// HTTPRouter (listener "main") — "POST <path>", mirroring
// HTTPTrigger.mountShared. auth "admin" still applies inside the
// handler; the shared router's own middleware (admin basic auth when
// configured) wraps it too — same credentials, so no double prompt.
func (n *SubmitNode) mountShared(pattern string, params []string) (nodestore.MessageInterface, error) {
	if n.host == nil || n.host.SharedHTTPRouter() == nil {
		return nil, fmt.Errorf("listener \"main\" requires a shared HTTP router on the host (host.SharedHTTPRouter()) — none is installed")
	}
	full := http.MethodPost + " " + pattern
	if err := n.host.SharedHTTPRouter().Handle(full, http.HandlerFunc(n.handleRequest)); err != nil {
		return nil, fmt.Errorf("cannot mount submit route %q on the shared listener: %w", full, err)
	}
	n.serverMu.Lock()
	defer n.serverMu.Unlock()
	if n.mounted != "" {
		n.host.SharedHTTPRouter().Unregister(full)
		return nil, fmt.Errorf("submit listener already mounted")
	}
	n.params = params
	n.router = n.host.SharedHTTPRouter()
	n.mounted = full
	return nodestore.NewMessage().SetPayload(map[string]interface{}{"status": "listening", "listener": "main"}).SetTimestamp(time.Now()), nil
}

// maxSizeBytes returns the configured upload cap (default applied).
func (n *SubmitNode) maxSizeBytes() int64 {
	if n.config.MaxSize > 0 {
		return n.config.MaxSize
	}
	return defaultMaxSubmitBytes
}

// handleRequest accepts one upload and injects it downstream. The
// response is a synchronous receipt — downstream processing is async.
func (n *SubmitNode) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed — submit accepts POST", http.StatusMethodNotAllowed)
		return
	}

	// Auth: "admin" requires a credential the host's authenticator
	// accepts; the username becomes msg.submit.submittedBy.
	submittedBy := ""
	if n.config.Auth == "admin" {
		if n.host == nil || n.host.HTTPAuthFunc() == nil {
			http.Error(w, "submit auth=admin but no authenticator is configured (set GOFLOW_AUTH_FILE)", http.StatusServiceUnavailable)
			return
		}
		user, ok := n.host.HTTPAuthFunc()(r)
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="goflow"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		submittedBy = user
	}

	maxBytes := n.maxSizeBytes()
	if r.ContentLength > maxBytes {
		http.Error(w, fmt.Sprintf("upload exceeds the %d byte limit", maxBytes), http.StatusRequestEntityTooLarge)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	data, filename, field, contentType, err := n.readUpload(r)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) || strings.Contains(err.Error(), "request body too large") {
			http.Error(w, fmt.Sprintf("upload exceeds the %d byte limit", maxBytes), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, fmt.Sprintf("invalid upload: %v", err), http.StatusBadRequest)
		return
	}

	submit := map[string]interface{}{
		"filename":    filename,
		"field":       field,
		"contentType": contentType,
		"size":        int64(len(data)),
		"submittedBy": submittedBy,
		"submittedAt": time.Now().UTC().Format(time.RFC3339Nano),
		"remoteAddr":  r.RemoteAddr,
	}
	if len(n.params) > 0 {
		params := make(map[string]string, len(n.params))
		for _, p := range n.params {
			params[p] = r.PathValue(p)
		}
		submit["params"] = params
	}

	payload := nodestore.NewMessage().SetPayload(data).SetMetadata(map[string]interface{}{
		"submit": submit,
	}).SetTimestamp(time.Now())

	// Fire-and-forget: the pending response channel is discarded — the
	// receipt answers intake; processing happens downstream.
	msgID, _, err := n.host.InjectMessage(n.workflowID, n.GetID(), payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	n.host.DiscardPending(msgID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"msgid": msgID,
		"size":  len(data),
	})
}

// readUpload extracts the uploaded bytes. Multipart requests deliver
// the first file part (name/filename/content-type from the part); any
// other body is taken raw with an optional ?filename= hint.
func (n *SubmitNode) readUpload(r *http.Request) (data []byte, filename, field, contentType string, err error) {
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaType == "multipart/form-data" {
		mr, merr := r.MultipartReader()
		if merr != nil {
			return nil, "", "", "", merr
		}
		for {
			part, perr := mr.NextPart()
			if perr == io.EOF {
				return nil, "", "", "", fmt.Errorf("multipart request contains no file part")
			}
			if perr != nil {
				return nil, "", "", "", perr
			}
			if part.FileName() == "" {
				continue // plain form field — the file part is what we want
			}
			b, rerr := io.ReadAll(part)
			if rerr != nil {
				return nil, "", "", "", rerr
			}
			ct := part.Header.Get("Content-Type")
			if ct == "" {
				ct = "application/octet-stream"
			}
			return b, part.FileName(), part.FormName(), ct, nil
		}
	}

	b, rerr := io.ReadAll(r.Body)
	if rerr != nil {
		return nil, "", "", "", rerr
	}
	ct := mediaType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return b, r.URL.Query().Get("filename"), "body", ct, nil
}

// Validate checks the node configuration (and normalizes defaults).
func (n *SubmitNode) Validate() error {
	if err := n.BaseNode.Validate(); err != nil {
		return err
	}
	if n.config.Path == "" {
		n.config.Path = "/submit"
	}
	switch n.config.Auth {
	case "", "none", "admin":
	default:
		return fmt.Errorf("invalid auth %q — expected \"none\" or \"admin\"", n.config.Auth)
	}
	switch n.config.Listener {
	case "", "own", "main":
	default:
		return fmt.Errorf("invalid listener %q — expected \"own\" or \"main\"", n.config.Listener)
	}
	if n.config.MaxSize < 0 {
		return fmt.Errorf("maxSize cannot be negative")
	}

	// Same probe as httpTrigger: reject paths whose mux pattern would
	// panic in HandleFunc (e.g. duplicate ":name" parameters).
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

// routePattern returns the ServeMux pattern and parameter names for
// the configured path (shared convention with httpTrigger).
func (n *SubmitNode) routePattern() (string, []string) {
	path := n.config.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return nodebase.MuxPattern(path)
}

// Stop shuts down the upload listener (idempotent).
func (n *SubmitNode) Stop() error {
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
		return fmt.Errorf("error shutting down submit listener: %w", err)
	}
	return nil
}
