package udpout

// Pack-local unit tests for the udpOut node — loopback datagram send,
// metadata ip/port override and config validation.

import (
	"context"
	"net"
	"strconv"

	"github.com/dracory/nodebase"
	"testing"
	"time"

	nodestore "github.com/dracory/nodestore"
)

func newNode(cfg *UDPOutConfig) *UDPOutNode {
	return NewUDPOutNode(nodestore.NewNode("u1", "udpOut").SetWorkflowID("wf"), cfg)
}

func listen(t *testing.T) (*net.UDPConn, int) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("udp listen failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, conn.LocalAddr().(*net.UDPAddr).Port
}

func read(t *testing.T, conn *net.UDPConn) []byte {
	t.Helper()
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	nr, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("no datagram received: %v", err)
	}
	return buf[:nr]
}

func TestUDPOutConfiguredTarget(t *testing.T) {
	conn, port := listen(t)
	n := newNode(&UDPOutConfig{Host: "127.0.0.1", Port: port})
	in := nodestore.NewMessage().SetPayload("datagram")
	out, err := n.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != in {
		t.Fatal("message should pass through")
	}
	if got := read(t, conn); string(got) != "datagram" {
		t.Fatalf("datagram wrong: %q", got)
	}
}

func TestUDPOutMetadataOverride(t *testing.T) {
	conn, port := listen(t)
	// config points nowhere valid; metadata wins
	n := newNode(&UDPOutConfig{Host: "127.0.0.1", Port: 1})
	in := nodestore.NewMessage().SetPayload("override").SetMetadata(map[string]interface{}{
		"ip": "127.0.0.1", "port": "5000",
	})
	in.GetMetadata()["port"] = strconv.Itoa(port)
	if _, err := n.Execute(context.Background(), in); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := read(t, conn); string(got) != "override" {
		t.Fatalf("datagram wrong: %q", got)
	}
}

func TestUDPOutNoPort(t *testing.T) {
	n := newNode(&UDPOutConfig{})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("x")); err == nil {
		t.Fatal("missing port should fail")
	}
}

func TestUDPOutAddrField(t *testing.T) {
	// Node-RED "addr" is the canonical target field; host is the alias.
	conn, port := listen(t)
	n := newNode(&UDPOutConfig{Addr: "127.0.0.1", Port: port})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("via-addr")); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := read(t, conn); string(got) != "via-addr" {
		t.Fatalf("datagram wrong: %q", got)
	}
}

func TestUDPOutBase64(t *testing.T) {
	conn, port := listen(t)
	n := newNode(&UDPOutConfig{Addr: "127.0.0.1", Port: port, Base64: true})
	// "aGk=" decodes to "hi"
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("aGk=")); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := read(t, conn); string(got) != "hi" {
		t.Fatalf("base64 datagram wrong: %q", got)
	}
}

func TestUDPOutBroadcast(t *testing.T) {
	// multicast "broad" defaults the address to the broadcast one;
	// sending must not error (receipt is platform-dependent).
	n := newNode(&UDPOutConfig{Multicast: "broad", Port: 9})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload("bcast")); err != nil {
		t.Fatalf("broadcast send failed: %v", err)
	}
}

func TestUDPOutNodeREDDecode(t *testing.T) {
	n := nodestore.NewNode("u1", "udpOut").SetWorkflowID("wf").SetData(map[string]interface{}{
		"addr": "localhost", "port": "5353", "ipv": "udp4",
		"outport": "", "base64": false, "multicast": "broad",
	})
	cfg := new(UDPOutConfig)
	if err := nodebase.DecodeConfig(n, cfg); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if cfg.Addr != "localhost" || cfg.Port != 5353 || cfg.Multicast != "broad" {
		t.Fatalf("decoded wrong: %+v", cfg)
	}
}

func TestUDPOutValidate(t *testing.T) {
	if err := newNode(&UDPOutConfig{Multicast: "bogus"}).Validate(); err == nil {
		t.Fatal("bad multicast should fail")
	}
	if err := newNode(&UDPOutConfig{Multicast: "broad"}).Validate(); err != nil {
		t.Fatalf("broad rejected: %v", err)
	}
}

func TestUDPOutEmptyPayload(t *testing.T) {
	n := newNode(&UDPOutConfig{Port: 1})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("nil payload should fail")
	}
}
