package udpout

// Pack-local unit tests for the udpOut node — loopback datagram send,
// metadata ip/port override and config validation.

import (
	"context"
	"net"
	"strconv"
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

func TestUDPOutEmptyPayload(t *testing.T) {
	n := newNode(&UDPOutConfig{Port: 1})
	if _, err := n.Execute(context.Background(), nodestore.NewMessage()); err == nil {
		t.Fatal("nil payload should fail")
	}
}
