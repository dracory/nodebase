package tcpout

// Pack-local unit tests for the tcpOut node — loopback dial mode,
// reply mode through the shared TCPSessions registry, and validation.

import (
	"context"
	"crypto/tls"
	"io"
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
		b, _ := io.ReadAll(conn)
		got <- b
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
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
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
	// no metadata
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("missing session metadata should fail")
	}
	// unknown session
	in := nodestore.NewMessage().SetPayload("x").SetMetadata(map[string]interface{}{
		"ip": "1.2.3.4", "port": "9",
	})
	if _, err := n.Execute(context.Background(), in); err == nil {
		t.Fatal("unknown session should fail")
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
