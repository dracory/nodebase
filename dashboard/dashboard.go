// Package dashboard is the runtime bridge half of the end-user
// dashboard for the nodestore stack: the state sink ui* widget
// executors push into (nodebase.BindUIWidgetSink / nodebase.PushUIState in
// nodes/uishare.go), the per-workflow widget manifest + retained
// state registry, and the websocket endpoint dashboard clients sync
// through. Dashboard PAGE serving (HTML shells, navigation) is a
// nodeadmin responsibility — this package is deliberately only the
// runtime state bridge.
//
// # Engine-agnostic by construction
//
// The hub holds a nodebase.NodeHost, never a concrete runner: no
// noderunner imports, so the package travels with the nodebase repo.
// Composition happens at the mount site — nodeadmin.New constructs
// the hub with the engine as Host and installs it as the engine's
// deploy observer (the hub's WorkflowActivated/WorkflowStopped
// methods satisfy noderunner.DeployObserver structurally; a host
// without an observer seam still works — scopes are created lazily
// through NodeHost.RunningFlow).
//
// # Wire protocol (JSON text frames on <mount>/ws?flow=ID)
//
// Server → client:
//
//	{"type":"manifest","workflow":"<id>","widgets":[...],"state":{"<node>":{...}}}
//	{"type":"update","node":"<nodeID>","state":{...}}
//	{"type":"close","reason":"undeployed"}
//	{"type":"error","message":"..."}
//
// Client → server:
//
//	{"type":"input","node":"<nodeID>","msg":{...}}      msg becomes msg.Payload
//	{"type":"input","node":"<nodeID>","payload":<any>}  shorthand; Data={"payload":v}
//
// "input" messages are injected via NodeHost.InjectMessage addressed
// from the widget node, so they travel downstream exactly as if the
// widget node emitted them. Only node ids present in the manifest are
// accepted.
//
// A JSON snapshot of a flow's manifest + state is also served at
// <mount>/manifest?flow=ID for page shells that render before
// opening the socket.
//
// Auth: when Options.Authenticator (or the host's HTTPAuthFunc) is
// set, both endpoints require it and failures get a Basic-auth
// challenge. With no authenticator the surface is open — same policy
// as the admin UI without credentials.
package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"

	"github.com/coder/websocket"
)

// Options configures the dashboard hub.
type Options struct {
	// Host is the NodeHost the dashboard runs against (required):
	// the seam widget executors push through and the hub injects
	// client input into.
	Host nodebase.NodeHost

	// Authenticator authenticates requests for both endpoints; nil
	// falls back to the host's HTTPAuthFunc. When both are nil the
	// dashboard is unauthenticated.
	Authenticator func(r *http.Request) (username string, ok bool)

	// AllowedOrigins restricts websocket upgrade Origin hosts
	// (websocket.Accept OriginPatterns, e.g. []string{"example.com",
	// "*.example.com"}). Empty keeps the legacy any-origin behaviour
	// (CSWSH exposure — set it on internet-facing deployments).
	AllowedOrigins []string
}

// New returns a *Hub serving the websocket bridge at <mount>/ws and
// a JSON manifest snapshot at <mount>/manifest, bound to the host's
// UIWidgetSink seam — after New, ui* executors on this host push
// state through the hub (see nodebase.BindUIWidgetSink).
//
// The caller wires run-lifecycle observation when the host supports
// it (e.g. engine.SetDeployObserver(hub) for noderunner); scopes are
// otherwise created lazily via Host.RunningFlow.
//
// Mount with a prefix strip so the hub stays mount-agnostic:
//
//	hub, err := dashboard.New(dashboard.Options{Host: engine})
//	mux.Handle("/dashboard/", http.StripPrefix("/dashboard", hub))
func New(opts Options) (*Hub, error) {
	if opts.Host == nil {
		return nil, errors.New("dashboard: Options.Host is required")
	}
	auth := opts.Authenticator
	if auth == nil {
		auth = opts.Host.HTTPAuthFunc()
	}
	h := &Hub{
		host:           opts.Host,
		auth:           auth,
		allowedOrigins: opts.AllowedOrigins,
		scopes:         map[string]*scope{},
		mux:            http.NewServeMux(),
	}
	h.mux.HandleFunc("GET /ws", h.ws)
	h.mux.HandleFunc("GET /manifest", h.manifest)
	// Bind host → hub so widget executors (whose factories see only
	// the host) push state via nodebase.PushUIState.
	h.unbind = nodebase.BindUIWidgetSink(opts.Host, h)
	return h, nil
}

// ServeHTTP authenticates then routes to the ws or manifest handler.
// Paths are relative to the mount point (see New).
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Baseline hardening headers on every dashboard response.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if h.auth != nil {
		if _, ok := h.auth(r); !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="nodebase"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
	}
	h.mux.ServeHTTP(w, r)
}

// manifest serves the JSON manifest + state snapshot for a flow —
// page shells fetch it before/alongside opening the socket.
func (h *Hub) manifest(w http.ResponseWriter, r *http.Request) {
	flowID, ok := h.resolveFlow(r)
	if !ok {
		http.Error(w, "dashboard: unknown or ambiguous workflow — pass ?flow=<id>", http.StatusNotFound)
		return
	}
	m, _ := h.Manifest(flowID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m)
}

// resolveFlow picks the workflow for a request: ?flow=ID wins; with
// no parameter a single running dashboard is picked automatically.
func (h *Hub) resolveFlow(r *http.Request) (string, bool) {
	if id := r.URL.Query().Get("flow"); id != "" {
		_, ok := h.Manifest(id)
		return id, ok
	}
	flows := h.Flows()
	if len(flows) == 1 {
		return flows[0], true
	}
	return "", false
}

// ws upgrades the connection, sends the manifest + state snapshot and
// then relays client "input" messages into the host.
func (h *Hub) ws(w http.ResponseWriter, r *http.Request) {
	flowID, ok := h.resolveFlow(r)
	if !ok {
		http.Error(w, "dashboard: unknown or ambiguous workflow — pass ?flow=<id>", http.StatusNotFound)
		return
	}

	accept := &websocket.AcceptOptions{}
	if len(h.allowedOrigins) > 0 {
		accept.OriginPatterns = h.allowedOrigins
	} else {
		// The dashboard page is same-origin, but flows may embed the
		// socket elsewhere; same legacy default as websocketIn.
		accept.InsecureSkipVerify = true //nolint:gosec — set Options.AllowedOrigins to restrict
	}
	conn, err := websocket.Accept(w, r, accept)
	if err != nil {
		return
	}

	client, manifest, ok := h.attach(flowID, conn)
	if !ok {
		_ = conn.Close(websocket.StatusInternalError, "workflow has no dashboard")
		return
	}
	defer client.Close()

	// The read loop below owns the connection lifetime; use a plain
	// background context for sends (client.Send serializes writers).
	ctx := context.Background()
	msg := struct {
		Type string `json:"type"`
		Manifest
	}{Type: "manifest", Manifest: manifest}
	if err := client.Send(ctx, msg); err != nil {
		return
	}

	// Read loop: every frame is a client→host interaction.
	for {
		var in struct {
			Type    string          `json:"type"`
			Node    string          `json:"node"`
			Msg     json.RawMessage `json:"msg"`
			Payload json.RawMessage `json:"payload"`
		}
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		if err := json.Unmarshal(data, &in); err != nil || in.Type != "input" {
			_ = client.Send(ctx, map[string]string{"type": "error", "message": "expected {\"type\":\"input\",\"node\":...}"})
			continue
		}
		if !h.HasWidget(flowID, in.Node) {
			_ = client.Send(ctx, map[string]string{"type": "error", "message": "unknown widget " + in.Node})
			continue
		}

		payload := nodestore.NewMessage().SetMetadata(map[string]interface{}{
			"source":           "dashboard",
			"dashboardSession": client.ID(),
			"dashboardWidget":  in.Node,
		}).SetTimestamp(time.Now())
		switch {
		case len(in.Msg) > 0:
			var m interface{}
			if err := json.Unmarshal(in.Msg, &m); err != nil {
				_ = client.Send(ctx, map[string]string{"type": "error", "message": "invalid msg JSON"})
				continue
			}
			payload.SetPayload(m)
		case len(in.Payload) > 0:
			var v interface{}
			if err := json.Unmarshal(in.Payload, &v); err != nil {
				_ = client.Send(ctx, map[string]string{"type": "error", "message": "invalid payload JSON"})
				continue
			}
			payload.SetPayload(map[string]interface{}{"payload": v})
		default:
			payload.SetPayload(map[string]interface{}{})
		}

		if _, _, err := h.host.InjectMessage(flowID, in.Node, payload); err != nil {
			_ = client.Send(ctx, map[string]string{"type": "error", "message": err.Error()})
		}
	}
}
