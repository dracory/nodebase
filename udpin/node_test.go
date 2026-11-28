package udpin

// Pack-local unit tests for the udpIn node — loopback datagram
// reception with sender ip/port metadata, and validation.

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// fakeHost captures emitted payloads.
type fakeHost struct {
	mu      sync.Mutex // guards emitted — the socket read loop emits from a goroutine
	emitted []nodestore.MessageInterface
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
	return f()
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

func TestUDPInReceives(t *testing.T) {
	// grab a free port first
	probe, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	port := probe.LocalAddr().(*net.UDPAddr).Port
	_ = probe.Close()

	host := &fakeHost{}
	n := NewUDPInNode(nodestore.NewNode("u1", "udpIn").SetWorkflowID("wf"), &UDPInConfig{Port: port}, host)
	defer func() { _ = n.Stop() }()

	out, err := n.Execute(context.Background(), nodestore.NewMessage())
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out.GetPayload().(map[string]interface{})["status"] != "listening" {
		t.Fatalf("status wrong: %v", out.GetPayload())
	}

	sender, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer func() { _ = sender.Close() }()
	_, _ = sender.Write([]byte("ping"))

	deadline := time.Now().Add(3 * time.Second)
	for len(host.emittedMsgs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(15 * time.Millisecond)
	}
	emitted := host.emittedMsgs()
	if len(emitted) == 0 {
		t.Fatal("no datagram emitted")
	}
	m := emitted[0]
	if m.GetPayload() != "ping" {
		t.Fatalf("payload wrong: %v", m.GetPayload())
	}
	if m.GetMetadata()["ip"] != "127.0.0.1" || m.GetMetadata()["port"] == "" {
		t.Fatalf("sender metadata wrong: %v", m.GetMetadata())
	}
}

func TestUDPInDatatypeBuffer(t *testing.T) {
	probe, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	port := probe.LocalAddr().(*net.UDPAddr).Port
	_ = probe.Close()

	host := &fakeHost{}
	n := NewUDPInNode(nodestore.NewNode("u1", "udpIn").SetWorkflowID("wf"),
		&UDPInConfig{Port: port, Datatype: "buffer"}, host)
	defer func() { _ = n.Stop() }()
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	sender, _ := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
	defer func() { _ = sender.Close() }()
	_, _ = sender.Write([]byte{0xDE, 0xAD})

	deadline := time.Now().Add(3 * time.Second)
	for len(host.emittedMsgs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(15 * time.Millisecond)
	}
	emitted := host.emittedMsgs()
	if len(emitted) == 0 {
		t.Fatal("no datagram emitted")
	}
	if b, ok := emitted[0].GetPayload().([]byte); !ok || len(b) != 2 || b[0] != 0xDE {
		t.Fatalf("buffer payload wrong: %#v", emitted[0].GetPayload())
	}
}

func TestUDPInMulticast(t *testing.T) {
	// multicast "true" + group joins a multicast group (Node-RED fields).
	probe, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	port := probe.LocalAddr().(*net.UDPAddr).Port
	_ = probe.Close()

	host := &fakeHost{}
	n := NewUDPInNode(nodestore.NewNode("u1", "udpIn").SetWorkflowID("wf"),
		&UDPInConfig{Port: port, Multicast: "true", Group: "224.0.0.251", Datatype: "utf8"}, host)
	defer func() { _ = n.Stop() }()
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err != nil {
		t.Skipf("multicast unavailable in this environment: %v", err)
	}
	sender, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("224.0.0.251"), Port: port})
	if err != nil {
		t.Skipf("multicast send unavailable: %v", err)
	}
	defer func() { _ = sender.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for len(host.emittedMsgs()) == 0 && time.Now().Before(deadline) {
		_, _ = sender.Write([]byte("mcast"))
		time.Sleep(100 * time.Millisecond)
	}
	emitted := host.emittedMsgs()
	if len(emitted) == 0 {
		t.Fatal("no multicast datagram emitted")
	}
	if emitted[0].GetPayload() != "mcast" {
		t.Fatalf("payload wrong: %v", emitted[0].GetPayload())
	}
}

func TestUDPInNodeREDDecode(t *testing.T) {
	n := nodestore.NewNode("u1", "udpIn").SetWorkflowID("wf").SetData(map[string]interface{}{
		"port": "5353", "ipv": "udp4", "multicast": "true",
		"group": "224.0.0.251", "iface": "", "datatype": "utf8",
	})
	cfg := new(UDPInConfig)
	if err := nodebase.DecodeConfig(n, cfg); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if cfg.Port != 5353 || cfg.Multicast != "true" || cfg.Group != "224.0.0.251" {
		t.Fatalf("decoded wrong: %+v", cfg)
	}
}

func TestUDPInValidate(t *testing.T) {
	host := &fakeHost{}
	if err := NewUDPInNode(nodestore.NewNode("u1", "udpIn"), &UDPInConfig{Port: 9999}, host).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := NewUDPInNode(nodestore.NewNode("u1", "udpIn"), &UDPInConfig{}, host).Validate(); err == nil {
		t.Fatal("missing port should fail")
	}
	if err := NewUDPInNode(nodestore.NewNode("u1", "udpIn"), &UDPInConfig{Port: 1, Multicast: "true"}, host).Validate(); err == nil {
		t.Fatal("multicast without group should fail")
	}
	if err := NewUDPInNode(nodestore.NewNode("u1", "udpIn"), &UDPInConfig{Port: 1, Multicast: "true", Group: "224.0.0.1"}, host).Validate(); err != nil {
		t.Fatalf("valid multicast rejected: %v", err)
	}
	if err := NewUDPInNode(nodestore.NewNode("u1", "udpIn"), &UDPInConfig{Port: 1, Datatype: "bogus"}, host).Validate(); err == nil {
		t.Fatal("bad datatype should fail")
	}
}
