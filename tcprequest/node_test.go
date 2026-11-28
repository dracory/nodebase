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
	t.Cleanup(func() { _ = ln.Close() })
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
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 64)
		_, _ = conn.Read(buf) // drain the request first so close isn't a RST
		_, _ = conn.Write([]byte("reply-data"))
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
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte("part1<END>extra ignored"))
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
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte("0123456789EXTRA"))
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
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 64)
		_, _ = conn.Read(buf) // drain the request first so close isn't a RST
		_, _ = conn.Write([]byte{0xDE, 0xAD})
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
		defer func() { _ = conn.Close() }()
		conns++
		buf := make([]byte, 64)
		for {
			nr, err := conn.Read(buf)
			if err != nil {
				return
			}
			_, _ = conn.Write([]byte(fmt.Sprintf("echo:%d\n", nr)))
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

func TestTCPRequestOutChar(t *testing.T) {
	// Node-RED out:"char" + splitc — reply ends at the delimiter byte.
	port := echoServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte("first\nsecond"))
	})
	n := newNode(&TCPRequestConfig{
		Server: "127.0.0.1", Port: port, Out: "char", Splitc: `\n`, Ret: "string", Timeout: 2,
	}, &fakeHost{})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("q"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "first\n" {
		t.Fatalf("char reply wrong: %v", out.GetPayload())
	}
}

func TestTCPRequestOutCount(t *testing.T) {
	// Node-RED out:"count" + splitc — reply is exactly N bytes.
	port := echoServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte("0123456789EXTRA"))
	})
	n := newNode(&TCPRequestConfig{
		Server: "127.0.0.1", Port: port, Out: "count", Splitc: "5", Ret: "string", Timeout: 2,
	}, &fakeHost{})
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("q"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "01234" {
		t.Fatalf("count reply wrong: %v", out.GetPayload())
	}
}

func TestTCPRequestOutImmed(t *testing.T) {
	// Node-RED out:"immed" — send and return without waiting.
	replied := make(chan struct{}, 1)
	port := echoServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 64)
		_, _ = conn.Read(buf)
		_, _ = conn.Write([]byte("too late"))
		replied <- struct{}{}
	})
	n := newNode(&TCPRequestConfig{
		Server: "127.0.0.1", Port: port, Out: "immed", Timeout: 2,
	}, &fakeHost{})
	in := nodestore.NewMessage().SetPayload("q")
	out, err := n.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != in {
		t.Fatal("immed should pass the message through")
	}
}

func TestTCPRequestMsgPortOverride(t *testing.T) {
	// Node-RED msg.port overrides the configured port.
	port := echoServer(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 64)
		_, _ = conn.Read(buf) // drain the request first so close isn't a RST
		_, _ = conn.Write([]byte("via-msg"))
	})
	in := nodestore.NewMessage().SetPayload("q").SetMetadata(map[string]interface{}{
		"host": "127.0.0.1", "port": fmt.Sprintf("%d", port),
	})
	// Config points at a dead port; metadata must win.
	n := newNode(&TCPRequestConfig{Server: "127.0.0.1", Port: 1, Timeout: 2}, &fakeHost{})
	out, err := n.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload() != "via-msg" {
		t.Fatalf("reply wrong: %v", out.GetPayload())
	}
}

func TestTCPRequestNodeREDDecode(t *testing.T) {
	// A Node-RED-shaped config map decodes onto the NR field names.
	n := nodestore.NewNode("r1", "tcpRequest").SetWorkflowID("wf").SetData(map[string]interface{}{
		"server": "10.1.2.3", "port": "9999", "out": "char", "ret": "buffer",
		"splitc": "\\n", "newline": "", "trim": false, "tls": "",
	})
	cfg := new(TCPRequestConfig)
	if err := nodebase.DecodeConfig(n, cfg); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if cfg.Server != "10.1.2.3" || cfg.Port != 9999 || cfg.Out != "char" || cfg.Splitc != "\\n" {
		t.Fatalf("decoded wrong: %+v", cfg)
	}
	r := cfg.resolve(nil)
	if r.mode != "delimiter" || r.delimiter != "\n" || r.host != "10.1.2.3" {
		t.Fatalf("resolve wrong: %+v", r)
	}
}

func TestTCPRequestDialFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen failed: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
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
