package nodebase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// wsSessions is the workflow-level registry of open websocket
// connections keyed by session id. It lives in host node state (like
// tcpSessions) so websocketOut — a different executor instance — can
// write back to the session that produced a message.
type WSSessions struct {
	mu    sync.Mutex
	conns map[string]*websocket.Conn
}

const WSSessionsKey = "__websocketSessions__"

// NewWSSessions returns an empty session registry (the value stored
// under WSSessionsKey in host node state).
func NewWSSessions() *WSSessions {
	return &WSSessions{conns: map[string]*websocket.Conn{}}
}

// Get returns the connection registered under id.
func (s *WSSessions) Get(id string) (*websocket.Conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.conns[id]
	return c, ok
}

// Put registers conn under id.
func (s *WSSessions) Put(id string, conn *websocket.Conn) {
	s.mu.Lock()
	s.conns[id] = conn
	s.mu.Unlock()
}

// DeleteIf removes id only when it still maps to conn.
func (s *WSSessions) DeleteIf(id string, conn *websocket.Conn) {
	s.mu.Lock()
	if s.conns[id] == conn {
		delete(s.conns, id)
	}
	s.mu.Unlock()
}

// All returns a snapshot of every registered connection.
func (s *WSSessions) All() []*websocket.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*websocket.Conn, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c)
	}
	return out
}

// CloseAll closes and removes every registered connection.
func (s *WSSessions) CloseAll() {
	s.CloseAllWith(websocket.StatusNormalClosure, "node stopped")
}

// CloseAllWith closes and removes every registered connection using
// the supplied close status and reason.
func (s *WSSessions) CloseAllWith(code websocket.StatusCode, reason string) {
	s.mu.Lock()
	for id, conn := range s.conns {
		_ = conn.Close(code, reason)
		delete(s.conns, id)
	}
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Shared websocket config nodes (Node-RED websocket-listener /
// websocket-client). Node-RED shares one listener or one outbound client
// connection between every websocket in/out node that references the
// config node; the state structs below live in host node state keyed by
// the config node's id, so any number of dependent nodes attach to the
// same mounted path / dialed connection.
// ---------------------------------------------------------------------------

// WebSocketListenerType is the engine type of the shared-listener
// config node (Node-RED "websocket-listener").
const WebSocketListenerType = "websocketListener"

// WebSocketClientType is the engine type of the shared-client config
// node (Node-RED "websocket-client").
const WebSocketClientType = "websocketClient"

// WSListenerConfig is the shared listener description a websocket
// in/out node references via its "server" field.
type WSListenerConfig struct {
	// Path is the endpoint path served on the host's shared HTTP
	// router (Node-RED serves it on the admin port).
	Path string `json:"path"`
	// WholeMsg selects Node-RED "entire message" mode; accepted for
	// parity, payload-only delivery is used either way.
	WholeMsg string `json:"wholemsg"`
}

// WSClientConfig is the shared outbound-client description a websocket
// in/out node references via its "client" field.
type WSClientConfig struct {
	// Path is the full ws:// or wss:// URL to dial.
	Path string `json:"path"`
	// TLS optionally references a tlsConfig node id.
	TLS string `json:"tls"`
	// WholeMsg selects Node-RED "entire message" mode; accepted for
	// parity, payload-only delivery is used either way.
	WholeMsg string `json:"wholemsg"`
}

// wsListenerStateKey / wsClientStateKey namespace the per-run state
// entries by config node id so a flow can carry several listeners and
// clients side by side.
func wsListenerStateKey(configNodeID string) string { return "__wsListener__:" + configNodeID }
func wsClientStateKey(configNodeID string) string   { return "__wsClient__:" + configNodeID }

// WSSub is the message sink a websocketIn registers on shared state:
// the connection's session id plus the received frame.
type WSSub func(sessionID string, typ websocket.MessageType, data []byte)

// WSListenerState is the runtime half of a websocketListener config
// node: the upgrade route mounted on the host's shared HTTP router, the
// accepted connections and every dependent node's subscription.
type WSListenerState struct {
	mu      sync.Mutex
	path    string
	router  HTTPRouter
	mounted bool
	closed  bool
	seq     int64
	conns   map[string]*websocket.Conn
	subs    map[string]WSSub
	refs    int
	// sessions exposes the mounted connections under a WSSessions
	// registry too, so websocketOut session mode keeps working on
	// listener-mode connections.
	sessions *WSSessions
}

// Attach mounts the upgrade handler on the host's shared HTTP router
// on first use and registers one more dependent. The listener model
// has no private port — a host without a shared router cannot serve it
// and the deploy fails loud.
func (s *WSListenerState) Attach(host NodeHost) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("websocketListener %q is closed", s.path)
	}
	if !s.mounted {
		router := host.SharedHTTPRouter()
		if router == nil {
			return fmt.Errorf("websocketListener %q requires a shared HTTP router on the host (host.SharedHTTPRouter()) — none is installed", s.path)
		}
		if err := router.Handle(s.path, http.HandlerFunc(s.handleUpgrade)); err != nil {
			return fmt.Errorf("websocketListener cannot mount %q: %w", s.path, err)
		}
		s.router = router
		s.mounted = true
	}
	s.refs++
	return nil
}

// Release detaches one dependent; the last release unmounts the route
// and closes every connection.
func (s *WSListenerState) Release() {
	s.mu.Lock()
	if s.refs > 0 {
		s.refs--
	}
	last := s.refs == 0
	s.mu.Unlock()
	if last {
		s.Close()
	}
}

// handleUpgrade accepts the connection, registers it, then fans every
// received frame out to the subscribed websocketIn nodes.
func (s *WSListenerState) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Browser flows may connect from any origin (Node-RED parity).
		InsecureSkipVerify: true, //nolint:gosec
	})
	if err != nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close(websocket.StatusGoingAway, "listener stopped")
		return
	}
	sessionID := fmt.Sprintf("ws-%d", atomic.AddInt64(&s.seq, 1))
	s.conns[sessionID] = conn
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.conns[sessionID] == conn {
			delete(s.conns, sessionID)
		}
		s.mu.Unlock()
		if s.sessions != nil {
			s.sessions.DeleteIf(sessionID, conn)
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}()

	for {
		typ, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		s.mu.Lock()
		subs := make([]WSSub, 0, len(s.subs))
		for _, sub := range s.subs {
			subs = append(subs, sub)
		}
		s.mu.Unlock()
		for _, sub := range subs {
			sub(sessionID, typ, data)
		}
	}
}

// Subscribe registers fn for frames received on this listener; id is
// the subscribing node's id so Unsubscribe can detach it.
func (s *WSListenerState) Subscribe(id string, fn WSSub) {
	s.mu.Lock()
	s.subs[id] = fn
	s.mu.Unlock()
}

// Unsubscribe detaches the subscriber registered under id.
func (s *WSListenerState) Unsubscribe(id string) {
	s.mu.Lock()
	delete(s.subs, id)
	s.mu.Unlock()
}

// Conns returns a snapshot of the mounted connection ids.
func (s *WSListenerState) Conns() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.conns))
	for id := range s.conns {
		out = append(out, id)
	}
	return out
}

// Conn returns the connection registered under sessionID.
func (s *WSListenerState) Conn(sessionID string) (*websocket.Conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.conns[sessionID]
	return c, ok
}

// Sessions returns the WSSessions view of the mounted connections.
func (s *WSListenerState) Sessions() *WSSessions { return s.sessions }

// Close unmounts the route and closes every connection (idempotent).
func (s *WSListenerState) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.mounted && s.router != nil {
		s.router.Unregister(s.path)
	}
	conns := s.conns
	s.conns = map[string]*websocket.Conn{}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close(websocket.StatusGoingAway, "listener stopped")
	}
}

// WSClientState is the runtime half of a websocketClient config node:
// one shared outbound connection plus the subscriptions of dependent
// websocketIn nodes.
type WSClientState struct {
	mu      sync.Mutex
	url     string
	conn    *websocket.Conn
	dialing bool
	closed  bool
	subs    map[string]WSSub
	refs    int
}

// Attach registers one more dependent on the shared connection.
func (s *WSClientState) Attach() {
	s.mu.Lock()
	s.refs++
	s.mu.Unlock()
}

// Release detaches one dependent; the last release drops the conn.
func (s *WSClientState) Release() {
	s.mu.Lock()
	if s.refs > 0 {
		s.refs--
	}
	last := s.refs == 0
	s.mu.Unlock()
	if last {
		s.Close()
	}
}

// Connect dials the configured URL once, then runs the read pump
// that fans received frames out to subscribers. A failed dial leaves
// the state unconnected; callers retry on their own schedule. Calls
// while another dial is in flight return nil immediately.
func (s *WSClientState) Connect(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("websocketClient %q is closed", s.url)
	}
	if s.conn != nil || s.dialing {
		s.mu.Unlock()
		return nil
	}
	s.dialing = true
	s.mu.Unlock()

	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	conn, _, err := websocket.Dial(dctx, s.url, nil)
	cancel()

	s.mu.Lock()
	s.dialing = false
	if err != nil || s.closed {
		s.mu.Unlock()
		if conn != nil {
			_ = conn.Close(websocket.StatusGoingAway, "client stopped")
		}
		return err
	}
	s.conn = conn
	s.mu.Unlock()
	go s.pump(conn)
	return nil
}

// pump reads until the peer or the network drops, then clears the
// shared conn so the next Connect re-dials.
func (s *WSClientState) pump(conn *websocket.Conn) {
	defer func() {
		s.mu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.mu.Unlock()
	}()
	for {
		typ, data, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		s.mu.Lock()
		subs := make([]WSSub, 0, len(s.subs))
		for _, sub := range s.subs {
			subs = append(subs, sub)
		}
		s.mu.Unlock()
		for _, sub := range subs {
			sub("", typ, data)
		}
	}
}

// Conn returns the dialed connection, or nil while unconnected.
func (s *WSClientState) Conn() *websocket.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// Subscribe registers fn for frames received on the client
// connection; id is the subscribing node's id.
func (s *WSClientState) Subscribe(id string, fn WSSub) {
	s.mu.Lock()
	s.subs[id] = fn
	s.mu.Unlock()
}

// Unsubscribe detaches the subscriber registered under id.
func (s *WSClientState) Unsubscribe(id string) {
	s.mu.Lock()
	delete(s.subs, id)
	s.mu.Unlock()
}

// Close drops the shared connection (idempotent).
func (s *WSClientState) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close(websocket.StatusGoingAway, "client stopped")
	}
}

// ResolveWSListener resolves ref to a websocketListener config node and
// returns its shared state plus decoded config. ref empty returns
// (nil, nil, nil) — the caller is not in shared-config mode.
func ResolveWSListener(host NodeHost, workflowID, ref string) (*WSListenerState, *WSListenerConfig, error) {
	if strings.TrimSpace(ref) == "" {
		return nil, nil, nil
	}
	node, ok := lookupConfigNode(host, workflowID, ref)
	if !ok || node == nil {
		return nil, nil, fmt.Errorf("websocket-listener %q not found in workflow", ref)
	}
	if node.GetType() != WebSocketListenerType {
		return nil, nil, fmt.Errorf("node %q is type %q, not %s", ref, node.GetType(), WebSocketListenerType)
	}
	raw, err := json.Marshal(node.GetData())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid %s data: %w", WebSocketListenerType, err)
	}
	cfg := &WSListenerConfig{}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, nil, fmt.Errorf("invalid %s data: %w", WebSocketListenerType, err)
	}
	path := cfg.Path
	if path == "" {
		path = "/ws"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	state := host.GetOrCreateNodeState(workflowID, wsListenerStateKey(ref), func() interface{} {
		return &WSListenerState{
			path:     path,
			conns:    map[string]*websocket.Conn{},
			subs:     map[string]WSSub{},
			sessions: NewWSSessions(),
		}
	}).(*WSListenerState)
	return state, cfg, nil
}

// ResolveWSClient resolves ref to a websocketClient config node and
// returns its shared state plus decoded config. ref empty returns
// (nil, nil, nil).
func ResolveWSClient(host NodeHost, workflowID, ref string) (*WSClientState, *WSClientConfig, error) {
	if strings.TrimSpace(ref) == "" {
		return nil, nil, nil
	}
	node, ok := lookupConfigNode(host, workflowID, ref)
	if !ok || node == nil {
		return nil, nil, fmt.Errorf("websocket-client %q not found in workflow", ref)
	}
	if node.GetType() != WebSocketClientType {
		return nil, nil, fmt.Errorf("node %q is type %q, not %s", ref, node.GetType(), WebSocketClientType)
	}
	raw, err := json.Marshal(node.GetData())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid %s data: %w", WebSocketClientType, err)
	}
	cfg := &WSClientConfig{}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, nil, fmt.Errorf("invalid %s data: %w", WebSocketClientType, err)
	}
	if cfg.Path == "" {
		return nil, nil, fmt.Errorf("%s %q has no ws:// URL (path)", WebSocketClientType, ref)
	}
	state := host.GetOrCreateNodeState(workflowID, wsClientStateKey(ref), func() interface{} {
		return &WSClientState{
			url:  cfg.Path,
			subs: map[string]WSSub{},
		}
	}).(*WSClientState)
	return state, cfg, nil
}
