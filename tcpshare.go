package nodebase

import (
	"net"
	"sync"
)

// tcpSessions is the workflow-level registry of accepted TCP
// connections, keyed by remote "ip:port". It lives in the host's
// node-state map under a reserved key so tcpOut can reply on the
// connection that produced a message (Node-RED _session semantics).
type TCPSessions struct {
	mu    sync.Mutex
	conns map[string]net.Conn
}

const TCPSessionsKey = "__tcpSessions__"

// NewTCPSessions returns an empty session registry (the value stored
// under TCPSessionsKey in host node state).
func NewTCPSessions() *TCPSessions {
	return &TCPSessions{conns: map[string]net.Conn{}}
}

// Get returns the connection registered under key.
func (s *TCPSessions) Get(key string) (net.Conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.conns[key]
	return c, ok
}

// Put registers conn under key.
func (s *TCPSessions) Put(key string, conn net.Conn) {
	s.mu.Lock()
	s.conns[key] = conn
	s.mu.Unlock()
}

// DeleteIf removes key only when it still maps to conn.
func (s *TCPSessions) DeleteIf(key string, conn net.Conn) {
	s.mu.Lock()
	if s.conns[key] == conn {
		delete(s.conns, key)
	}
	s.mu.Unlock()
}

// CloseAll closes and removes every registered connection.
func (s *TCPSessions) CloseAll() {
	s.mu.Lock()
	for key, conn := range s.conns {
		_ = conn.Close()
		delete(s.conns, key)
	}
	s.mu.Unlock()
}
