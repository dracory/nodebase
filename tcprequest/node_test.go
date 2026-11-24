package tcprequest

// Pack-local unit tests for the tcpRequest node — request/reply over
// a loopback echo server covering all three reply framings plus
// keep-alive reuse and validation.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost carries the node-state slot keep mode uses.
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

func newNode(cfg *TCPRequestConfig, host *fakeHost) *TCPRequestNode {
	return NewTCPRequestNode(nodestore.NewNode("r1", "tcpRequest").SetWorkflowID("wf"), cfg, host)
}

// echoServer replies to each accepted connection per the handler.
func echoServer(t *testing.T, handle func(net.Conn)) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handle(conn)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestTCPRequestTimeoutMode(t *testing.T) {
	port := echoServer(t, func(conn net.Conn) {
		defer conn.Close()
		buf := make([]byte, 64)
		conn.Read(buf) // drain the request first so close isn't a RST
		conn.Write([]byte("reply-data"))
	})
	n := newNode(&TCPRequestConfig{Host: "127.0.0.1", Port: port, Timeout: 2}, &fakeHost{})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("q"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "reply-data" {
		t.Fatalf("reply wrong: %v", out.GetPayload())
	}
	if out.GetMetadata()["ip"] == "" || out.GetMetadata()["port"] == "" {
		t.Fatalf("remote metadata missing: %v", out.GetMetadata())
	}
}

func TestTCPRequestDelimiterMode(t *testing.T) {
	port := echoServer(t, func(conn net.Conn) {
		defer conn.Close()
		conn.Write([]byte("part1<END>extra ignored"))
	})
	n := newNode(&TCPRequestConfig{
		Host: "127.0.0.1", Port: port, Mode: "delimiter", Delimiter: "<END>", Timeout: 2,
	}, &fakeHost{})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("q"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "part1<END>" {
		t.Fatalf("delimiter reply wrong: %v", out.GetPayload())
	}
}

func TestTCPRequestLengthMode(t *testing.T) {
	port := echoServer(t, func(conn net.Conn) {
		defer conn.Close()
		conn.Write([]byte("0123456789EXTRA"))
	})
	n := newNode(&TCPRequestConfig{
		Host: "127.0.0.1", Port: port, Mode: "length", Length: 5, Timeout: 2,
	}, &fakeHost{})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("q"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "01234" {
		t.Fatalf("length reply wrong: %v", out.GetPayload())
	}
}

func TestTCPRequestBufferRet(t *testing.T) {
	port := echoServer(t, func(conn net.Conn) {
		defer conn.Close()
		buf := make([]byte, 64)
		conn.Read(buf) // drain the request first so close isn't a RST
		conn.Write([]byte{0xDE, 0xAD})
	})
	n := newNode(&TCPRequestConfig{Host: "127.0.0.1", Port: port, Ret: "buffer", Timeout: 2}, &fakeHost{})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("q"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if b, ok := out.GetPayload().([]byte); !ok || len(b) != 2 || b[0] != 0xDE {
		t.Fatalf("buffer ret wrong: %v", out.GetPayload())
	}
}

func TestTCPRequestKeepAlive(t *testing.T) {
	conns := 0
	port := echoServer(t, func(conn net.Conn) {
		defer conn.Close()
		conns++
		buf := make([]byte, 64)
		for {
			nr, err := conn.Read(buf)
			if err != nil {
				return
			}
			conn.Write([]byte(fmt.Sprintf("echo:%d\n", nr)))
		}
	})
	host := &fakeHost{}
	n := newNode(&TCPRequestConfig{
		Host: "127.0.0.1", Port: port, Keep: true, Mode: "delimiter", Delimiter: `\n`, Timeout: 2,
	}, host)
	for i := 0; i < 2; i++ {
		out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("ping"))
		if err != nil {
			t.Fatalf("execute %d failed: %v", i, err)
		}
		if out.GetPayload() != "echo:4\n" {
			t.Fatalf("reply %d wrong: %v", i, out.GetPayload())
		}
	}
	if conns != 1 {
		t.Fatalf("keep mode should reuse one connection, saw %d", conns)
	}
}

func TestTCPRequestDialFailure(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	n := newNode(&TCPRequestConfig{Host: "127.0.0.1", Port: port, Timeout: 1}, &fakeHost{})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("unreachable host should fail")
	}
}

func TestTCPRequestValidate(t *testing.T) {
	host := &fakeHost{}
	if err := newNode(&TCPRequestConfig{Port: 1}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&TCPRequestConfig{}, host).Validate(); err == nil {
		t.Fatal("missing port should fail")
	}
	if err := newNode(&TCPRequestConfig{Port: 1, Mode: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad mode should fail")
	}
	if err := newNode(&TCPRequestConfig{Port: 1, Mode: "delimiter"}, host).Validate(); err == nil {
		t.Fatal("empty delimiter should fail")
	}
	if err := newNode(&TCPRequestConfig{Port: 1, Mode: "length"}, host).Validate(); err == nil {
		t.Fatal("zero length should fail")
	}
	if err := newNode(&TCPRequestConfig{Port: 1, Ret: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad ret should fail")
	}
}
