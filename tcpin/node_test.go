package tcpin

// Pack-local unit tests for the tcpIn node — loopback listen-mode
// line emission with remote ip/port metadata and session
// registration, plus connect mode and validation.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost captures emitted payloads and keeps node state.
type fakeHost struct {
	mu      sync.Mutex // guards emitted/state — connections emit from accept/read goroutines
	emitted []nodestore.MessageInterface
	state   map[string]interface{}
}

func (h *fakeHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
	return nil
}

func (h *fakeHost) emittedMsgs() []nodestore.MessageInterface {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nodestore.MessageInterface(nil), h.emitted...)
}

func (h *fakeHost) nodeState(key string) interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state[key]
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
	h.mu.Lock()
	defer h.mu.Unlock()
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

func newNode(cfg *TCPInConfig, host *fakeHost) *TCPInNode {
	return NewTCPInNode(nodestore.NewNode("t1", "tcpIn").SetWorkflowID("wf"), cfg, host)
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(15 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestTCPInListen(t *testing.T) {
	host := &fakeHost{}
	n := newNode(&TCPInConfig{Mode: "listen", Port: 0}, host)
	defer n.Stop()

	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["status"] != "listening" {
		t.Fatalf("status wrong: %v", out.GetPayload())
	}
	port := n.listener.Addr().(*net.TCPAddr).Port

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	conn.Write([]byte("line one\nline two\n"))

	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 2 }, "line emission")
	emitted := host.emittedMsgs()
	if emitted[0].GetPayload() != "line one" {
		t.Fatalf("line wrong: %v", emitted[0].GetPayload())
	}
	m := emitted[0].GetMetadata()
	if m["ip"] != "127.0.0.1" || m["port"] == "" {
		t.Fatalf("remote metadata wrong: %v", m)
	}
	// session registered for tcpOut reply mode
	sessions := host.nodeState("wf:" + nodebase.TCPSessionsKey).(*nodebase.TCPSessions)
	key := fmt.Sprintf("%v:%v", m["ip"], m["port"])
	if _, ok := sessions.Get(key); !ok {
		t.Fatalf("session not registered under %q", key)
	}
}

func TestTCPInConnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("server line\n"))
		time.Sleep(100 * time.Millisecond)
	}()

	host := &fakeHost{}
	port := ln.Addr().(*net.TCPAddr).Port
	n := newNode(&TCPInConfig{Mode: "connect", Host: "127.0.0.1", Port: port}, host)
	defer n.Stop()
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["status"] != "connected" {
		t.Fatalf("status wrong: %v", out.GetPayload())
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "connect-mode emission")
	if m := host.emittedMsgs()[0]; m.GetPayload() != "server line" {
		t.Fatalf("payload wrong: %v", m.GetPayload())
	}
}

func TestTCPInConnectFailure(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	n := newNode(&TCPInConfig{Mode: "connect", Host: "127.0.0.1", Port: port}, &fakeHost{})
	defer n.Stop()
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("unreachable host should fail")
	}
}

// listenAndDial starts a datamode-configured tcpIn on a free port and
// returns the client side of a loopback connection.
func listenAndDial(t *testing.T, cfg *TCPInConfig, host *fakeHost) (*TCPInNode, net.Conn) {
	t.Helper()
	probe, _ := net.Listen("tcp", "127.0.0.1:0")
	port := probe.Addr().(*net.TCPAddr).Port
	probe.Close()
	cfg.Server = "server"
	cfg.Port = port
	n := newNode(cfg, host)
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		n.Stop()
		t.Fatalf("activate failed: %v", err)
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		n.Stop()
		t.Fatalf("dial failed: %v", err)
	}
	return n, conn
}

func TestTCPInSingleMode(t *testing.T) {
	// datamode "single": the whole buffer emits once at close.
	host := &fakeHost{}
	nl := ""
	n, conn := listenAndDial(t, &TCPInConfig{Datamode: "single", Datatype: "utf8", Newline: &nl}, host)
	defer n.Stop()
	conn.Write([]byte("chunk one "))
	conn.Write([]byte("chunk two"))
	conn.Close()

	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "single-mode emission")
	emitted := host.emittedMsgs()
	if len(emitted) != 1 {
		t.Fatalf("single mode should emit once, got %d", len(emitted))
	}
	if emitted[0].GetPayload() != "chunk one chunk two" {
		t.Fatalf("payload wrong: %v", emitted[0].GetPayload())
	}
}

func TestTCPInStreamChunks(t *testing.T) {
	// buffer datatype emits each received chunk as raw bytes.
	host := &fakeHost{}
	n, conn := listenAndDial(t, &TCPInConfig{Datamode: "stream", Datatype: "buffer"}, host)
	defer n.Stop()
	conn.Write([]byte{1, 2, 3})
	time.Sleep(50 * time.Millisecond)
	conn.Write([]byte{4, 5})
	conn.Close()

	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 2 }, "stream chunk emission")
	emitted := host.emittedMsgs()
	if b, ok := emitted[0].GetPayload().([]byte); !ok || len(b) != 3 || b[0] != 1 {
		t.Fatalf("chunk 1 wrong: %#v", emitted[0].GetPayload())
	}
	if b, ok := emitted[1].GetPayload().([]byte); !ok || len(b) != 2 || b[0] != 4 {
		t.Fatalf("chunk 2 wrong: %#v", emitted[1].GetPayload())
	}
}

func TestTCPInDelimiterChar(t *testing.T) {
	// utf8 stream with a custom delimiter character (Node-RED newline).
	host := &fakeHost{}
	nl := "|"
	n, conn := listenAndDial(t, &TCPInConfig{Datamode: "stream", Datatype: "utf8", Newline: &nl}, host)
	defer n.Stop()
	conn.Write([]byte("aa|bb|cc"))
	conn.Close()

	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 2 }, "delimited emission")
	emitted := host.emittedMsgs()
	if emitted[0].GetPayload() != "aa" || emitted[1].GetPayload() != "bb" {
		t.Fatalf("parts wrong: %v / %v", emitted[0].GetPayload(), emitted[1].GetPayload())
	}
	// The unterminated remainder flushes at close (Node-RED 'end').
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 3 }, "remainder flush")
	if host.emittedMsgs()[2].GetPayload() != "cc" {
		t.Fatalf("remainder wrong: %v", host.emittedMsgs()[2].GetPayload())
	}
}

func TestTCPInTrimReattach(t *testing.T) {
	// trim re-attaches the delimiter to each emitted part.
	host := &fakeHost{}
	nl := "|"
	n, conn := listenAndDial(t, &TCPInConfig{Datamode: "stream", Datatype: "utf8", Newline: &nl, Trim: true}, host)
	defer n.Stop()
	conn.Write([]byte("aa|bb|"))
	conn.Close()

	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 2 }, "trim emission")
	emitted := host.emittedMsgs()
	if emitted[0].GetPayload() != "aa|" || emitted[1].GetPayload() != "bb|" {
		t.Fatalf("parts wrong: %v / %v", emitted[0].GetPayload(), emitted[1].GetPayload())
	}
}

func TestTCPInLengthMode(t *testing.T) {
	// datamode "length": emit every N bytes; remainder at close.
	host := &fakeHost{}
	n, conn := listenAndDial(t, &TCPInConfig{Datamode: "length", Datatype: "utf8", Length: 4}, host)
	defer n.Stop()
	conn.Write([]byte("aaaabbbbcc"))
	conn.Close()

	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 3 }, "length-mode emission")
	emitted := host.emittedMsgs()
	if emitted[0].GetPayload() != "aaaa" || emitted[1].GetPayload() != "bbbb" || emitted[2].GetPayload() != "cc" {
		t.Fatalf("chunks wrong: %v / %v / %v", emitted[0].GetPayload(), emitted[1].GetPayload(), emitted[2].GetPayload())
	}
}

func TestTCPInTimeoutMode(t *testing.T) {
	// datamode "timeout": buffered data flushes after FlushMS idle.
	host := &fakeHost{}
	n, conn := listenAndDial(t, &TCPInConfig{Datamode: "timeout", Datatype: "utf8", FlushMS: 80}, host)
	defer n.Stop()
	conn.Write([]byte("first burst"))
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "timeout flush")
	if host.emittedMsgs()[0].GetPayload() != "first burst" {
		t.Fatalf("flush wrong: %v", host.emittedMsgs()[0].GetPayload())
	}
	conn.Write([]byte("second"))
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 2 }, "second timeout flush")
	if host.emittedMsgs()[1].GetPayload() != "second" {
		t.Fatalf("second flush wrong: %v", host.emittedMsgs()[1].GetPayload())
	}
	conn.Close()
}

func TestTCPInBase64Datatype(t *testing.T) {
	host := &fakeHost{}
	n, conn := listenAndDial(t, &TCPInConfig{Datamode: "stream", Datatype: "base64"}, host)
	defer n.Stop()
	conn.Write([]byte("hi"))
	conn.Close()

	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "base64 emission")
	if host.emittedMsgs()[0].GetPayload() != "aGk=" {
		t.Fatalf("base64 wrong: %v", host.emittedMsgs()[0].GetPayload())
	}
}

func TestTCPInServerField(t *testing.T) {
	// Node-RED "server":"client" selects connect mode (overrides mode).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("hello\n"))
		time.Sleep(50 * time.Millisecond)
	}()

	host := &fakeHost{}
	port := ln.Addr().(*net.TCPAddr).Port
	n := newNode(&TCPInConfig{Server: "client", Mode: "listen", Host: "127.0.0.1", Port: port}, host)
	defer n.Stop()
	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["status"] != "connected" {
		t.Fatalf("server:client should connect: %v", out.GetPayload())
	}
	waitFor(t, func() bool { return len(host.emittedMsgs()) >= 1 }, "connect emission")
}

func TestTCPInValidate(t *testing.T) {
	host := &fakeHost{}
	if err := newNode(&TCPInConfig{Mode: "listen", Port: 5000}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := newNode(&TCPInConfig{Mode: "listen"}, host).Validate(); err == nil {
		t.Fatal("missing port should fail")
	}
	if err := newNode(&TCPInConfig{Mode: "bogus", Port: 1}, host).Validate(); err == nil {
		t.Fatal("bad mode should fail")
	}
}
