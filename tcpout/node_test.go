package tcpout

// Pack-local unit tests for the tcpOut node — loopback dial mode,
// reply mode through the shared TCPSessions registry, and validation.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost carries the node-state slot tcpOut reply mode reads.
type fakeHost struct {
	state map[string]interface{}
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	return nil
}
func (h *fakeHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *fakeHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *fakeHost) ResolveResponse(id string, p nodestore.MessageInterface) error { return nil }
func (h *fakeHost) DiscardPending(id string)                                      {}
func (h *fakeHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) {
	return nil, false
}
func (h *fakeHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) { return nil, false }
func (h *fakeHost) RunContext(wf string) context.Context                      { return context.Background() }
func (h *fakeHost) GetOrCreateNodeState(wf, nodeID string, f func() interface{}) interface{} {
	key := wf + ":" + nodeID
	if h.state == nil {
		h.state = map[string]interface{}{}
	}
	if v, ok := h.state[key]; ok {
		return v
	}
	v := f()
	h.state[key] = v
	return v
}
func (h *fakeHost) ExecuteDownstream(ctx context.Context, wf, from string, w nodestore.WorkflowInterface, p nodestore.MessageInterface, ports []string) {
}
func (h *fakeHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) { return nil, false }
func (h *fakeHost) ResolvedContextStore() nodebase.ContextStore         { return nil }
func (h *fakeHost) LoadTLSConfig(wf, ref string) (*tls.Config, error)   { return nil, nil }
func (h *fakeHost) FileRootPath() string                                { return "" }
func (h *fakeHost) ExecAllowed() bool                                   { return false }
func (h *fakeHost) SharedHTTPRouter() nodebase.HTTPRouter               { return nil }
func (h *fakeHost) HTTPAuthFunc() func(r *http.Request) (string, bool)  { return nil }
func (h *fakeHost) QueueService() nodebase.QueueService                 { return nil }

func newNode(cfg *TCPOutConfig, host *fakeHost) *TCPOutNode {
	return NewTCPOutNode(nodestore.NewNode("t1", "tcpOut").SetWorkflowID("wf"), cfg, host)
}

func newNodeID(id string, cfg *TCPOutConfig, host *fakeHost) *TCPOutNode {
	return NewTCPOutNode(nodestore.NewNode(id, "tcpOut").SetWorkflowID("wf"), cfg, host)
}

func TestTCPOutDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	got := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// client mode keeps the connection open — read one chunk
		// rather than ReadAll (which would wait for a close).
		b := make([]byte, 64)
		nr, _ := conn.Read(b)
		got <- b[:nr]
	}()

	port := ln.Addr().(*net.TCPAddr).Port
	n := newNode(&TCPOutConfig{Host: "127.0.0.1", Port: port}, &fakeHost{})
	in := nodestore.NewMessage().SetPayload("hello tcp")
	out, err := n.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != in {
		t.Fatal("message should pass through")
	}
	select {
	case b := <-got:
		if string(b) != "hello tcp" {
			t.Fatalf("bytes wrong: %q", b)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no data received")
	}
}

func TestTCPOutDialFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen failed: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close() // refuse connections
	n := newNode(&TCPOutConfig{Host: "127.0.0.1", Port: port}, &fakeHost{})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("unreachable target should fail")
	}
}

func TestTCPOutReply(t *testing.T) {
	// build a real loopback pair and register the server side as the
	// session the reply mode looks up
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	got := make(chan []byte, 1)
	go func() {
		b := make([]byte, 64)
		nr, _ := clientConn.Read(b)
		got <- b[:nr]
	}()

	host := &fakeHost{}
	sessions := host.GetOrCreateNodeState("wf", nodebase.TCPSessionsKey, func() interface{} {
		return nodebase.NewTCPSessions()
	}).(*nodebase.TCPSessions)
	sessions.Put("10.0.0.1:5000", serverConn)

	n := newNode(&TCPOutConfig{Reply: true}, host)
	in := nodestore.NewMessage().SetPayload("pong").SetMetadata(map[string]interface{}{
		"ip": "10.0.0.1", "port": "5000",
	})
	if _, err := n.Execute(context.Background(), in); err != nil {
		t.Fatalf("reply failed: %v", err)
	}
	select {
	case b := <-got:
		if string(b) != "pong" {
			t.Fatalf("reply bytes wrong: %q", b)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no reply data")
	}
}

func TestTCPOutReplyErrors(t *testing.T) {
	n := newNode(&TCPOutConfig{Reply: true}, &fakeHost{})
	// No session metadata broadcasts to every pooled connection
	// (Node-RED connectionPool semantics) — an empty pool is fine.
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err != nil {
		t.Fatalf("broadcast to empty pool should pass: %v", err)
	}
	// unknown session
	in := nodestore.NewMessage().SetPayload("x").SetMetadata(map[string]interface{}{
		"ip": "1.2.3.4", "port": "9",
	})
	if _, err := n.Execute(context.Background(), in); err == nil {
		t.Fatal("unknown session should fail")
	}
}

func TestTCPOutReplyBroadcast(t *testing.T) {
	// Two pooled sessions, no message metadata — the reply writes to
	// every pooled connection (Node-RED connectionPool semantics).
	c1a, c1b := net.Pipe()
	defer c1a.Close()
	defer c1b.Close()
	c2a, c2b := net.Pipe()
	defer c2a.Close()
	defer c2b.Close()
	got := make(chan []byte, 2)
	read := func(c net.Conn) {
		b := make([]byte, 64)
		nr, _ := c.Read(b)
		got <- b[:nr]
	}
	go read(c1a)
	go read(c2a)

	host := &fakeHost{}
	sessions := host.GetOrCreateNodeState("wf", nodebase.TCPSessionsKey, func() interface{} {
		return nodebase.NewTCPSessions()
	}).(*nodebase.TCPSessions)
	sessions.Put("10.0.0.1:5000", c1b)
	sessions.Put("10.0.0.2:5000", c2b)

	n := newNode(&TCPOutConfig{Beserver: "reply"}, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("all")); err != nil {
		t.Fatalf("broadcast failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case b := <-got:
			if string(b) != "all" {
				t.Fatalf("broadcast bytes wrong: %q", b)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("missing broadcast write")
		}
	}
}

func TestTCPOutClientPersistent(t *testing.T) {
	// client mode (Node-RED beserver:"client") keeps one connection
	// across messages; end:true closes it after each write.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	conns := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns <- conn
		}
	}()

	port := ln.Addr().(*net.TCPAddr).Port
	host := &fakeHost{}
	n := newNode(&TCPOutConfig{Beserver: "client", Host: "127.0.0.1", Port: port}, host)
	defer n.Stop()
	read := func(t *testing.T, conn net.Conn, want string) {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		b := make([]byte, 64)
		nr, err := conn.Read(b)
		if err != nil || string(b[:nr]) != want {
			t.Fatalf("want %q, got %q err=%v", want, b[:nr], err)
		}
	}

	// First message dials; second message reuses the same socket.
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("one")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	conn := <-conns
	defer conn.Close()
	read(t, conn, "one")
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("two")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	read(t, conn, "two")

	// end:true closes after the write; the next message re-dials.
	n2 := newNodeID("t2", &TCPOutConfig{Beserver: "client", Host: "127.0.0.1", Port: port, End: true}, host)
	defer n2.Stop()
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("bye")); err != nil {
		t.Fatalf("end write: %v", err)
	}
	conn2 := <-conns
	defer conn2.Close()
	read(t, conn2, "bye")
	_ = conn2.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn2.Read(make([]byte, 1)); err == nil {
		t.Fatal("end:true should close the connection after the write")
	}
	if _, err := n2.Execute(context.Background(), nodestore.NewMessage().SetPayload("again")); err != nil {
		t.Fatalf("redial write: %v", err)
	}
	conn3 := <-conns
	defer conn3.Close()
	read(t, conn3, "again")
}

func TestTCPOutServerBroadcast(t *testing.T) {
	// server mode (Node-RED beserver:"server") listens and writes
	// each message to every connected socket.
	host := &fakeHost{}
	n := newNode(&TCPOutConfig{Beserver: "server", Port: 0}, host)
	// Port 0 won't pass Validate; pick a real free port.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen failed: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	probe.Close()
	n.config.Port = port
	defer n.Stop()
	if err := n.Activate(context.Background()); err != nil {
		t.Fatalf("activate failed: %v", err)
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		n.state.mu.Lock()
		count := len(n.state.conns)
		n.state.mu.Unlock()
		if count == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("bcast")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	b := make([]byte, 64)
	nr, err := conn.Read(b)
	if err != nil || string(b[:nr]) != "bcast" {
		t.Fatalf("broadcast wrong: %q err=%v", b[:nr], err)
	}
}

func TestTCPOutValidate(t *testing.T) {
	host := &fakeHost{}
	if err := newNode(&TCPOutConfig{Port: 5000}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&TCPOutConfig{}, host).Validate(); err == nil {
		t.Fatal("missing port should fail")
	}
	if err := newNode(&TCPOutConfig{Reply: true}, host).Validate(); err != nil {
		t.Fatalf("reply mode needs no port: %v", err)
	}
}

func TestTCPSessions(t *testing.T) {
	s := nodebase.NewTCPSessions()
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	s.Put("k", c1)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("session not stored")
	}
	s.DeleteIf("k", c2) // wrong conn — must not delete
	if _, ok := s.Get("k"); !ok {
		t.Fatal("DeleteIf removed wrong entry")
	}
	s.DeleteIf("k", c1)
	if _, ok := s.Get("k"); ok {
		t.Fatal("DeleteIf did not remove")
	}
	s.Put("a", c1)
	s.CloseAll()
	if _, ok := s.Get("a"); ok {
		t.Fatal("CloseAll left entries")
	}
}
