package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"

	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"

	"github.com/coder/websocket"
)

// Manifest is the snapshot a connecting dashboard client receives:
// the workflow's widget list plus each widget's last pushed state
// (late-join replay).
type Manifest struct {
	Workflow string                            `json:"workflow"`
	Widgets  []nodebase.UIWidgetManifest          `json:"widgets"`
	State    map[string]map[string]interface{} `json:"state"`
}

// session is one connected dashboard client. writeMu serializes conn
// writes (coder/websocket allows a single concurrent writer) between
// hub broadcasts and handler-originated messages.
type session struct {
	id      string
	conn    *websocket.Conn
	writeMu sync.Mutex
}

// send writes one JSON text frame to the session.
func (s *session) send(ctx context.Context, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.conn.Write(ctx, websocket.MessageText, data)
}

// scope is the per-workflow slice of the hub: widget manifest,
// last-state map (node id → state) and open sessions.
type scope struct {
	widgets  []nodebase.UIWidgetManifest
	widgetBy map[string]bool
	state    map[string]map[string]interface{}
	sessions map[string]*session
	seq      int64
}

// Hub is the dashboard runtime bridge: the nodebase.UIWidgetSink ui*
// executors push state into, the per-workflow registry of widget
// manifests + retained state + client sessions, and the websocket
// endpoint clients sync through. It holds only a nodebase.NodeHost —
// it is engine-agnostic and travels with the nodebase.
//
// The hub satisfies the run-lifecycle observer contract structurally
// (WorkflowActivated/WorkflowStopped take nodestore types only): a
// host that can install a deploy observer (e.g. noderunner's
// engine.SetDeployObserver) wires it at mount time; nodeadmin does
// so. Scope creation also happens lazily via host.RunningFlow, so a
// host without an observer seam still serves dashboards for running
// flows.
type Hub struct {
	host   nodebase.NodeHost
	auth   func(r *http.Request) (string, bool)
	mux    *http.ServeMux
	unbind func()

	mu     sync.Mutex
	scopes map[string]*scope
}

// WorkflowActivated is the deploy-observer notification a host wires
// (structurally compatible with noderunner.DeployObserver): the
// workflow's widget manifest is (re)published so connected clients
// see the fresh layout after any deploy — initial, full or partial.
func (h *Hub) WorkflowActivated(workflow nodestore.WorkflowInterface) {
	h.setManifest(workflow.GetID(), nodebase.CollectUIWidgets(workflow))
}

// WorkflowStopped is the deploy-observer notification fired on
// undeploy (StopWorkflow and failed-activation alike): the
// workflow's scope is torn down.
func (h *Hub) WorkflowStopped(workflowID string) {
	h.clear(workflowID)
}

// setManifest installs the widget manifest for a running workflow.
// Existing sessions stay connected and receive the fresh manifest —
// this is the redeploy path. State for removed widgets is pruned.
func (h *Hub) setManifest(workflowID string, widgets []nodebase.UIWidgetManifest) {
	h.mu.Lock()
	sc := h.scopes[workflowID]
	if sc == nil {
		sc = &scope{
			state:    map[string]map[string]interface{}{},
			sessions: map[string]*session{},
		}
		h.scopes[workflowID] = sc
	}
	sc.widgets = widgets
	sc.widgetBy = make(map[string]bool, len(widgets))
	for _, w := range widgets {
		sc.widgetBy[w.ID] = true
	}
	for nodeID := range sc.state {
		if !sc.widgetBy[nodeID] {
			delete(sc.state, nodeID)
		}
	}
	manifest := Manifest{Workflow: workflowID, Widgets: widgets, State: copyState(sc.state)}
	var sessions []*session
	for _, s := range sc.sessions {
		sessions = append(sessions, s)
	}
	h.mu.Unlock()

	msg := struct {
		Type string `json:"type"`
		Manifest
	}{Type: "manifest", Manifest: manifest}
	for _, s := range sessions {
		_ = s.send(context.Background(), msg)
	}
}

// clear tears down a workflow's scope: clients get a "close" message,
// sockets are closed with StatusGoingAway, state is dropped. Called
// via WorkflowStopped on undeploy and failed activation.
func (h *Hub) clear(workflowID string) {
	h.mu.Lock()
	sc := h.scopes[workflowID]
	delete(h.scopes, workflowID)
	h.mu.Unlock()
	if sc == nil {
		return
	}
	for _, s := range sc.sessions {
		_ = s.send(context.Background(), map[string]string{
			"type":   "close",
			"reason": "undeployed",
		})
		_ = s.conn.Close(websocket.StatusGoingAway, "workflow undeployed")
	}
}

// UpdateWidget implements nodebase.UIWidgetSink: stores the widget's
// state and broadcasts it to every session of the workflow — the
// engine→client half of the dashboard contract. Widget executors
// reach it through nodebase.PushUIState. Unknown workflows are resolved
// lazily via host.RunningFlow (executor pushes only happen inside a
// run, so the flow is running by definition); a host that reports
// the flow not running drops the update.
func (h *Hub) UpdateWidget(workflowID, nodeID string, state map[string]interface{}) {
	sc := h.scopeFor(workflowID)
	if sc == nil {
		return
	}
	h.mu.Lock()
	sc = h.scopes[workflowID]
	if sc == nil {
		h.mu.Unlock()
		return
	}
	sc.state[nodeID] = state
	var sessions []*session
	for _, s := range sc.sessions {
		sessions = append(sessions, s)
	}
	h.mu.Unlock()

	msg := map[string]interface{}{
		"type":  "update",
		"node":  nodeID,
		"state": state,
	}
	for _, s := range sessions {
		if err := s.send(context.Background(), msg); err != nil {
			// Dead socket — the read loop's detach also removes it;
			// dropping here just makes the cleanup eager.
			h.detach(workflowID, s)
		}
	}
}

// scopeFor returns the workflow's scope, creating it lazily from
// host.RunningFlow when the host reports the flow running (covers
// hosts that never wired a deploy observer). Nil when the workflow
// is not running on this host.
func (h *Hub) scopeFor(workflowID string) *scope {
	h.mu.Lock()
	if sc := h.scopes[workflowID]; sc != nil {
		h.mu.Unlock()
		return sc
	}
	h.mu.Unlock()

	workflow, ok := h.host.RunningFlow(workflowID)
	if !ok || workflow == nil {
		return nil
	}
	h.setManifest(workflowID, nodebase.CollectUIWidgets(workflow))
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.scopes[workflowID]
}

// attach registers conn as a dashboard session for workflowID and
// returns the manifest snapshot plus the session handle. ok=false
// when the workflow has no dashboard scope (not running or no
// widgets).
func (h *Hub) attach(workflowID string, conn *websocket.Conn) (*client, Manifest, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sc := h.scopes[workflowID]
	if sc == nil {
		return nil, Manifest{}, false
	}
	sc.seq++
	s := &session{
		id:   fmt.Sprintf("dash-%d", sc.seq),
		conn: conn,
	}
	sc.sessions[s.id] = s
	manifest := Manifest{
		Workflow: workflowID,
		Widgets:  append([]nodebase.UIWidgetManifest(nil), sc.widgets...),
		State:    copyState(sc.state),
	}
	return &client{session: s, hub: h, flow: workflowID}, manifest, true
}

// detach removes a session (called when the read loop ends or a
// broadcast finds a dead socket).
func (h *Hub) detach(workflowID string, s *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if sc := h.scopes[workflowID]; sc != nil {
		if sc.sessions[s.id] == s {
			delete(sc.sessions, s.id)
		}
	}
}

// Manifest returns the current widget manifest + state for a running
// workflow, resolving the scope lazily (see scopeFor). ok=false when
// the workflow has no dashboard scope. Used to pick the default flow
// and to validate widget ids for client input.
func (h *Hub) Manifest(workflowID string) (Manifest, bool) {
	h.scopeFor(workflowID)
	h.mu.Lock()
	defer h.mu.Unlock()
	sc := h.scopes[workflowID]
	if sc == nil {
		return Manifest{}, false
	}
	return Manifest{
		Workflow: workflowID,
		Widgets:  append([]nodebase.UIWidgetManifest(nil), sc.widgets...),
		State:    copyState(sc.state),
	}, true
}

// HasWidget reports whether nodeID is a widget in the workflow's
// current manifest — client "input" messages may only target
// widgets.
func (h *Hub) HasWidget(workflowID, nodeID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	sc := h.scopes[workflowID]
	return sc != nil && sc.widgetBy[nodeID]
}

// Flows lists the workflow IDs that currently have a dashboard —
// used to resolve the dashboard when no ?flow= is given
// (single-dashboard deployments pick it automatically).
func (h *Hub) Flows() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]string, 0, len(h.scopes))
	for id := range h.scopes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Close unbinds the hub from the sink seam and closes every session
// — call on host teardown.
func (h *Hub) Close() {
	if h.unbind != nil {
		h.unbind()
		h.unbind = nil
	}
	h.mu.Lock()
	scopes := h.scopes
	h.scopes = map[string]*scope{}
	h.mu.Unlock()
	for _, sc := range scopes {
		for _, s := range sc.sessions {
			_ = s.conn.Close(websocket.StatusGoingAway, "dashboard closed")
		}
	}
}

// client is a connected dashboard websocket session — the ws handler
// writes protocol messages through it so broadcasts and direct sends
// share one write mutex.
type client struct {
	session *session
	hub     *Hub
	flow    string
}

// Send writes one JSON protocol message to the client.
func (c *client) Send(ctx context.Context, v interface{}) error {
	return c.session.send(ctx, v)
}

// ID returns the session id (stamped into injected message metadata).
func (c *client) ID() string { return c.session.id }

// Close detaches and closes the socket.
func (c *client) Close() {
	c.hub.detach(c.flow, c.session)
	_ = c.session.conn.Close(websocket.StatusNormalClosure, "")
}

func copyState(in map[string]map[string]interface{}) map[string]map[string]interface{} {
	out := make(map[string]map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
