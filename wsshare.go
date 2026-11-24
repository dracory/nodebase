package nodebase

import (
	"sync"

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
