package dashboard

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"

	"github.com/coder/websocket"
)

// stubHost is a minimal nodebase.NodeHost standing in for a runner: a
// set of "running" workflows plus a recorded InjectMessage log. All
// other NodeHost surface is inert — the hub only needs RunningFlow,
// InjectMessage and HTTPAuthFunc.
type stubHost struct {
	flows    map[string]nodestore.WorkflowInterface
	injected []injectedMsg
}

type injectedMsg struct {
	workflowID string
	nodeID     string
	payload    nodestore.MessageInterface
}

func newStubHost() *stubHost {
	return &stubHost{flows: map[string]nodestore.WorkflowInterface{}}
}

func (h *stubHost) addFlow(wf nodestore.WorkflowInterface) {
	h.flows[wf.GetID()] = wf
}

func (h *stubHost) Emit(workflowID, nodeID string, payload nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *stubHost) EmitEvent(workflowID, nodeID, nodeName, level, message string) {}
func (h *stubHost) InjectMessage(workflowID, fromNodeID string, payload nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	h.injected = append(h.injected, injectedMsg{workflowID, fromNodeID, payload})
	return "msg-1", nil, nil
}
func (h *stubHost) ResolveResponse(msgID string, payload nodestore.MessageInterface) error {
	return nil
}
func (h *stubHost) DiscardPending(msgID string) {}
func (h *stubHost) LookupNode(workflowID, nodeID string) (nodestore.NodeInterface, bool) {
	return nil, false
}
func (h *stubHost) RunningFlow(workflowID string) (nodestore.WorkflowInterface, bool) {
	wf, ok := h.flows[workflowID]
	return wf, ok
}
func (h *stubHost) RunContext(workflowID string) context.Context { return context.Background() }
func (h *stubHost) GetOrCreateNodeState(workflowID, nodeID string, factory func() interface{}) interface{} {
	return factory()
}
func (h *stubHost) ExecuteDownstream(ctx context.Context, workflowID, fromNodeID string, workflow nodestore.WorkflowInterface, payload nodestore.MessageInterface, ports []string) {
}
func (h *stubHost) NodeEnv(workflowID, nodeID, name string) (interface{}, bool) {
	return nil, false
}
func (h *stubHost) ResolvedContextStore() nodebase.ContextStore { return nil }
func (h *stubHost) LoadTLSConfig(workflowID, ref string) (*tls.Config, error) {
	return nil, nil
}
func (h *stubHost) FileRootPath() string                             { return "" }
func (h *stubHost) ExecAllowed() bool                                { return false }
func (h *stubHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *stubHost) QueueService() nodebase.QueueService                 { return nil }
func (h *stubHost) HTTPAuthFunc() func(*http.Request) (string, bool) { return nil }

// newHub wires a stub host + constructed hub.
func newHub(t *testing.T, host *stubHost) *Hub {
	t.Helper()
	hub, err := New(Options{Host: host})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(hub.Close)
	return hub
}

// widgetFlow builds a deployed workflow with one uiText and one
// uiButton widget plus a non-widget node.
func widgetFlow(id string) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow(id).SetID(id).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("t1", "uiText").SetData(map[string]interface{}{
			"page": "main", "label": "Readout",
		}),
		nodestore.NewNode("b1", "uiButton").SetData(map[string]interface{}{
			"page": "main", "payload": "go",
		}),
		nodestore.NewNode("d1", "debug").SetData(map[string]interface{}{}),
	})
}

// dialWS opens a test server + client websocket for hub at the given
// query (e.g. "?flow=wf1").
func dialWS(t *testing.T, hub *Hub, query string) (*websocket.Conn, func()) {
	t.Helper()
	srv := httptest.NewServer(hub)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws" + query
	conn, _, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		srv.Close()
		t.Fatalf("Dial %s: %v", url, err)
	}
	return conn, srv.Close
}

// readMsg reads one JSON protocol frame into a map.
func readMsg(t *testing.T, conn *websocket.Conn) map[string]interface{} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	return m
}

// TestManifestDescribe checks the manifest snapshot carries widget
// manifests (layout + type config) and skips non-widget nodebase.
func TestManifestDescribe(t *testing.T) {
	host := newStubHost()
	wf := widgetFlow("wf1")
	host.addFlow(wf)
	hub := newHub(t, host)
	hub.WorkflowActivated(wf)

	m, ok := hub.Manifest("wf1")
	if !ok {
		t.Fatal("Manifest: no scope for wf1")
	}
	if len(m.Widgets) != 2 {
		t.Fatalf("want 2 widgets, got %d: %+v", len(m.Widgets), m.Widgets)
	}
	if m.Widgets[0].ID != "b1" || m.Widgets[1].ID != "t1" {
		t.Fatalf("widget order wrong: %+v", m.Widgets)
	}
	if m.Widgets[0].Config["payload"] != "go" {
		t.Fatalf("uiButton config payload missing: %+v", m.Widgets[0].Config)
	}
	if m.Widgets[1].Label != "Readout" || m.Widgets[1].Page != "main" {
		t.Fatalf("uiText layout missing: %+v", m.Widgets[1])
	}
	if !hub.HasWidget("wf1", "t1") || hub.HasWidget("wf1", "d1") {
		t.Fatal("HasWidget wrong")
	}
}

// TestLazyScopeFromRunningFlow proves the hub needs no deploy
// observer: a scope appears from host.RunningFlow alone.
func TestLazyScopeFromRunningFlow(t *testing.T) {
	host := newStubHost()
	host.addFlow(widgetFlow("wf1"))
	hub := newHub(t, host)

	// No WorkflowActivated call — Manifest resolves via RunningFlow.
	m, ok := hub.Manifest("wf1")
	if !ok || len(m.Widgets) != 2 {
		t.Fatalf("lazy manifest failed: ok=%v widgets=%d", ok, len(m.Widgets))
	}
	if _, ok := hub.Manifest("nope"); ok {
		t.Fatal("unknown flow should have no scope")
	}
}

// TestStateReachesClient is the core acceptance test: a widget state
// push via the nodebase.PushUIState seam lands on a connected dashboard
// websocket client as an "update" frame.
func TestStateReachesClient(t *testing.T) {
	host := newStubHost()
	wf := widgetFlow("wf1")
	host.addFlow(wf)
	hub := newHub(t, host)
	hub.WorkflowActivated(wf)

	conn, closeSrv := dialWS(t, hub, "?flow=wf1")
	defer closeSrv()
	defer conn.Close(websocket.StatusNormalClosure, "")

	first := readMsg(t, conn)
	if first["type"] != "manifest" {
		t.Fatalf("want manifest first, got %v", first["type"])
	}
	if first["workflow"] != "wf1" {
		t.Fatalf("manifest workflow wrong: %v", first["workflow"])
	}

	// Push through the real seam — this is what a ui* executor does.
	nodebase.PushUIState(host, "wf1", "t1", map[string]interface{}{"value": "42"})

	upd := readMsg(t, conn)
	if upd["type"] != "update" || upd["node"] != "t1" {
		t.Fatalf("want update for t1, got %v", upd)
	}
	state, _ := upd["state"].(map[string]interface{})
	if state["value"] != "42" {
		t.Fatalf("state payload wrong: %v", state)
	}

	// Late join replays retained state in the manifest.
	m, _ := hub.Manifest("wf1")
	if m.State["t1"]["value"] != "42" {
		t.Fatalf("retained state missing: %v", m.State)
	}
}

// TestLateJoinReplay: a client connecting after a push gets the
// retained state inside the manifest frame.
func TestLateJoinReplay(t *testing.T) {
	host := newStubHost()
	wf := widgetFlow("wf1")
	host.addFlow(wf)
	hub := newHub(t, host)
	hub.WorkflowActivated(wf)

	nodebase.PushUIState(host, "wf1", "t1", map[string]interface{}{"value": "early"})

	conn, closeSrv := dialWS(t, hub, "?flow=wf1")
	defer closeSrv()
	defer conn.Close(websocket.StatusNormalClosure, "")

	m := readMsg(t, conn)
	state, _ := m["state"].(map[string]interface{})
	t1, _ := state["t1"].(map[string]interface{})
	if t1["value"] != "early" {
		t.Fatalf("late-join replay missing state: %v", m)
	}
}

// TestClientInputInjection: a client "input" frame is injected into
// the host downstream of the addressed widget with dashboard
// metadata stamped.
func TestClientInputInjection(t *testing.T) {
	host := newStubHost()
	wf := widgetFlow("wf1")
	host.addFlow(wf)
	hub := newHub(t, host)
	hub.WorkflowActivated(wf)

	conn, closeSrv := dialWS(t, hub, "?flow=wf1")
	defer closeSrv()
	defer conn.Close(websocket.StatusNormalClosure, "")
	_ = readMsg(t, conn) // manifest

	frame := `{"type":"input","node":"b1","payload":"go"}`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Give the read loop a beat to process.
	deadline := time.Now().Add(2 * time.Second)
	for len(host.injected) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(host.injected) != 1 {
		t.Fatalf("want 1 injected message, got %d", len(host.injected))
	}
	got := host.injected[0]
	if got.workflowID != "wf1" || got.nodeID != "b1" {
		t.Fatalf("injection addressed wrong: %+v", got)
	}
	meta := got.payload.GetMetadata()
	if meta["source"] != "dashboard" || meta["dashboardWidget"] != "b1" {
		t.Fatalf("metadata not stamped: %v", meta)
	}

	// Input to a non-widget node is rejected with an error frame.
	bad := `{"type":"input","node":"d1","payload":"x"}`
	if err := conn.Write(ctx, websocket.MessageText, []byte(bad)); err != nil {
		t.Fatalf("write: %v", err)
	}
	errMsg := readMsg(t, conn)
	if errMsg["type"] != "error" {
		t.Fatalf("want error frame, got %v", errMsg)
	}
}

// TestUndeployClosesClients: WorkflowStopped sends "close" and tears
// the scope down.
func TestUndeployClosesClients(t *testing.T) {
	host := newStubHost()
	wf := widgetFlow("wf1")
	host.addFlow(wf)
	hub := newHub(t, host)
	hub.WorkflowActivated(wf)

	conn, closeSrv := dialWS(t, hub, "?flow=wf1")
	defer closeSrv()
	defer conn.Close(websocket.StatusNormalClosure, "")
	_ = readMsg(t, conn)

	// A real host stops reporting the flow running on undeploy.
	delete(host.flows, "wf1")
	hub.WorkflowStopped("wf1")
	msg := readMsg(t, conn)
	if msg["type"] != "close" {
		t.Fatalf("want close frame, got %v", msg)
	}
	if _, ok := hub.Manifest("wf1"); ok {
		t.Fatal("scope should be gone after stop")
	}
}

// TestRedeployPushesFreshManifest: WorkflowActivated with a changed
// widget set pushes a fresh manifest to connected clients.
func TestRedeployPushesFreshManifest(t *testing.T) {
	host := newStubHost()
	wf := widgetFlow("wf1")
	host.addFlow(wf)
	hub := newHub(t, host)
	hub.WorkflowActivated(wf)

	conn, closeSrv := dialWS(t, hub, "?flow=wf1")
	defer closeSrv()
	defer conn.Close(websocket.StatusNormalClosure, "")
	_ = readMsg(t, conn)

	wf2 := nodestore.NewWorkflow("wf1").SetID("wf1").SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("g1", "uiGauge").SetData(map[string]interface{}{"min": 0, "max": 10}),
	})
	host.addFlow(wf2)
	hub.WorkflowActivated(wf2)

	m := readMsg(t, conn)
	if m["type"] != "manifest" {
		t.Fatalf("want manifest push, got %v", m)
	}
	widgets, _ := m["widgets"].([]interface{})
	if len(widgets) != 1 {
		t.Fatalf("want 1 widget after redeploy, got %v", widgets)
	}
}

// TestDisabledWidgetSkipped: a disabled widget node is excluded from
// the manifest (same predicate the engine deploy path applies).
func TestDisabledWidgetSkipped(t *testing.T) {
	host := newStubHost()
	wf := nodestore.NewWorkflow("wf1").SetID("wf1").SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("t1", "uiText").SetData(map[string]interface{}{"d": true}),
		nodestore.NewNode("t2", "uiText").SetData(map[string]interface{}{}),
	})
	host.addFlow(wf)
	hub := newHub(t, host)
	hub.WorkflowActivated(wf)

	m, ok := hub.Manifest("wf1")
	if !ok || len(m.Widgets) != 1 || m.Widgets[0].ID != "t2" {
		t.Fatalf("disabled node leaked into manifest: %+v", m.Widgets)
	}
}

// TestResolveFlowDefault: with exactly one dashboard the ?flow=
// parameter may be omitted.
func TestResolveFlowDefault(t *testing.T) {
	host := newStubHost()
	wf := widgetFlow("only")
	host.addFlow(wf)
	hub := newHub(t, host)
	hub.WorkflowActivated(wf)

	conn, closeSrv := dialWS(t, hub, "")
	defer closeSrv()
	defer conn.Close(websocket.StatusNormalClosure, "")
	m := readMsg(t, conn)
	if m["workflow"] != "only" {
		t.Fatalf("default flow resolution failed: %v", m)
	}
}
