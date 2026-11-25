// Package mqtttestbroker is a minimal in-process MQTT 3.1.1 broker
// for tests. It implements just enough of the protocol for real
// clients to connect (plain TCP or TLS), subscribe, publish, receive
// publishes, ping and disconnect — including last-will delivery on
// ungraceful drops. It is a test double: no auth, no retain store,
// no QoS 2, no persistence — do not use it in production.
package mqtttestbroker

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"
)

// packet types (MQTT 3.1.1 §2.2)
const (
	pktCONNECT    = 1
	pktCONNACK    = 2
	pktPUBLISH    = 3
	pktPUBACK     = 4
	pktSUBSCRIBE  = 8
	pktSUBACK     = 9
	pktPINGREQ    = 12
	pktPINGRESP   = 13
	pktDISCONNECT = 14
)

// subscription is one topic filter a client subscribed to.
type subscription struct {
	filter string
	qos    byte
}

// client is one broker-side connection.
type client struct {
	conn  net.Conn
	subs  []subscription
	will  *willMessage
	clean bool
}

// willMessage is the last-will registered at CONNECT time.
type willMessage struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

// Broker is a running test broker.
type Broker struct {
	mu      sync.Mutex
	ln      net.Listener
	clients map[net.Conn]*client
	closed  bool
	// Published records every publish the broker saw, in order —
	// handy for assertions on what the client side sent.
	Published []PublishedMessage
	// tlsPEM holds the CA PEM when the broker runs TLS — tests write
	// it to a file to exercise the pem-path config of the nodes.
	caPEM []byte
}

// PublishedMessage is one publish observed by the broker.
type PublishedMessage struct {
	Topic   string
	Payload []byte
	// QoS and Retain are the flags the publisher sent on the wire —
	// recorded so tests can assert what the client side published.
	QoS    byte
	Retain bool
}

// Start runs a plaintext broker on a random loopback port.
func Start() (*Broker, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return serve(ln)
}

// StartTLS runs a TLS broker on a random loopback port using a
// self-signed server certificate valid for 127.0.0.1/localhost. The
// CA PEM is available via CAPEM so tests can configure client-side
// verification through the same file-path knobs real flows use.
func StartTLS() (*Broker, error) {
	cert, caPEM, err := makeServerCert()
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	tlsLn := tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	b, err := serve(tlsLn)
	if err != nil {
		return nil, err
	}
	b.caPEM = caPEM
	return b, nil
}

// Addr is the broker address in host:port form.
func (b *Broker) Addr() string { return b.ln.Addr().String() }

// CAPEM returns the PEM bytes of the CA that signed the server
// certificate (TLS brokers only — nil otherwise).
func (b *Broker) CAPEM() []byte { return b.caPEM }

// Close stops the listener and drops every client connection.
func (b *Broker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	conns := make([]net.Conn, 0, len(b.clients))
	for c := range b.clients {
		conns = append(conns, c)
	}
	b.mu.Unlock()
	_ = b.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

func serve(ln net.Listener) (*Broker, error) {
	b := &Broker{
		ln:      ln,
		clients: map[net.Conn]*client{},
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go b.handle(conn)
		}
	}()
	return b, nil
}

// handle reads and answers packets on one client connection until it
// drops. DISCONNECT is a graceful close (no will); any other end —
// read error or EOF — is ungraceful and triggers the will.
func (b *Broker) handle(conn net.Conn) {
	c := &client{conn: conn}
	b.mu.Lock()
	b.clients[conn] = c
	b.mu.Unlock()

	graceful := false
	for {
		typ, flags, body, err := readPacket(conn)
		if err != nil {
			break // ungraceful drop
		}
		switch typ {
		case pktCONNECT:
			will, err := parseConnect(body)
			if err != nil {
				break
			}
			c.will = will
			if writePacket(conn, pktCONNACK, 0, []byte{0x00, 0x00}) != nil {
				break
			}
		case pktSUBSCRIBE:
			subs, packetID, err := parseSubscribe(flags, body)
			if err != nil {
				break
			}
			c.subs = append(c.subs, subs...)
			ack := []byte{byte(packetID >> 8), byte(packetID)}
			for _, s := range subs {
				ack = append(ack, s.qos)
			}
			_ = writePacket(conn, pktSUBACK, 0, ack)
		case pktPUBLISH:
			topic, packetID, payload := parsePublish(flags, body)
			qos := (flags >> 1) & 0x03
			if qos == 1 && packetID != 0 {
				_ = writePacket(conn, pktPUBACK, 0, []byte{byte(packetID >> 8), byte(packetID)})
			}
			b.mu.Lock()
			b.Published = append(b.Published, PublishedMessage{
				Topic:   topic,
				Payload: payload,
				QoS:     qos,
				Retain:  flags&0x01 != 0,
			})
			b.mu.Unlock()
			b.broadcast(topic, payload)
		case pktPINGREQ:
			_ = writePacket(conn, pktPINGRESP, 0, nil)
		case pktDISCONNECT:
			graceful = true
			goto done
		}
	}
done:
	b.mu.Lock()
	delete(b.clients, conn)
	b.mu.Unlock()
	_ = conn.Close()
	if !graceful && c.will != nil && c.will.topic != "" {
		b.broadcast(c.will.topic, c.will.payload)
	}
}

// broadcast forwards one published message to every client with a
// matching subscription, as QoS 0.
func (b *Broker) broadcast(topic string, payload []byte) {
	b.mu.Lock()
	targets := make([]net.Conn, 0, len(b.clients))
	for conn, c := range b.clients {
		for _, s := range c.subs {
			if topicMatch(s.filter, topic) {
				targets = append(targets, conn)
				break
			}
		}
	}
	b.mu.Unlock()
	body := append(encodeString(topic), payload...)
	for _, conn := range targets {
		_ = writePacket(conn, pktPUBLISH, 0, body)
	}
}

// topicMatch applies MQTT wildcard rules: + matches one level, #
// matches the rest of the topic from its level on.
func topicMatch(filter, topic string) bool {
	if filter == "#" {
		return true
	}
	fp := strings.Split(filter, "/")
	tp := strings.Split(topic, "/")
	for i, seg := range fp {
		if seg == "#" {
			return true
		}
		if i >= len(tp) {
			return false
		}
		if seg != "+" && seg != tp[i] {
			return false
		}
	}
	return len(fp) == len(tp)
}

// readPacket reads one MQTT control packet.
func readPacket(r io.Reader) (typ byte, flags byte, body []byte, err error) {
	var head [1]byte
	if _, err = io.ReadFull(r, head[:]); err != nil {
		return 0, 0, nil, err
	}
	typ = head[0] >> 4
	flags = head[0] & 0x0F
	length := 0
	shift := uint(0)
	for {
		var lb [1]byte
		if _, err = io.ReadFull(r, lb[:]); err != nil {
			return 0, 0, nil, err
		}
		length |= int(lb[0]&0x7F) << shift
		if lb[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 21 {
			return 0, 0, nil, errors.New("malformed remaining length")
		}
	}
	body = make([]byte, length)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, 0, nil, err
	}
	return typ, flags, body, nil
}

// writePacket writes one MQTT control packet.
func writePacket(w io.Writer, typ byte, flags byte, body []byte) error {
	out := []byte{typ<<4 | flags}
	length := len(body)
	for {
		d := byte(length % 128)
		length /= 128
		if length > 0 {
			d |= 0x80
		}
		out = append(out, d)
		if length == 0 {
			break
		}
	}
	out = append(out, body...)
	_, err := w.Write(out)
	return err
}

// parseConnect extracts the last-will from a CONNECT payload. The
// leading protocol name/level/flags/keepalive are validated loosely —
// clients that speak garbage are not this harness's concern.
func parseConnect(body []byte) (*willMessage, error) {
	if len(body) < 10 {
		return nil, errors.New("short CONNECT")
	}
	// skip protocol name string + level byte
	pos := 2 + int(binary.BigEndian.Uint16(body[0:2]))
	if pos+4 > len(body) {
		return nil, errors.New("short CONNECT header")
	}
	pos++ // protocol level
	flags := body[pos]
	pos++    // connect flags
	pos += 2 // keepalive
	// client id
	if pos+2 > len(body) {
		return nil, errors.New("short CONNECT payload")
	}
	idLen := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2 + idLen
	if flags&0x04 == 0 {
		return nil, nil // no will
	}
	willQoS := (flags >> 3) & 0x03
	willRetain := flags&0x20 != 0
	if pos+2 > len(body) {
		return nil, errors.New("short will topic")
	}
	tLen := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	if pos+tLen+2 > len(body) {
		return nil, errors.New("short will payload")
	}
	topic := string(body[pos : pos+tLen])
	pos += tLen
	pLen := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	if pos+pLen > len(body) {
		return nil, errors.New("short will payload body")
	}
	payload := append([]byte(nil), body[pos:pos+pLen]...)
	return &willMessage{topic: topic, payload: payload, qos: willQoS, retain: willRetain}, nil
}

// parseSubscribe reads a SUBSCRIBE payload into subscriptions plus the
// packet id.
func parseSubscribe(flags byte, body []byte) ([]subscription, int, error) {
	if len(body) < 4 {
		return nil, 0, errors.New("short SUBSCRIBE")
	}
	packetID := int(binary.BigEndian.Uint16(body[0:2]))
	pos := 2
	var subs []subscription
	for pos+3 <= len(body) {
		fLen := int(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		if pos+fLen+1 > len(body) {
			return nil, 0, errors.New("short SUBSCRIBE filter")
		}
		filter := string(body[pos : pos+fLen])
		pos += fLen
		qos := body[pos] & 0x03
		pos++
		subs = append(subs, subscription{filter: filter, qos: qos})
	}
	if len(subs) == 0 {
		return nil, 0, errors.New("SUBSCRIBE without filters")
	}
	return subs, packetID, nil
}

// parsePublish extracts topic, packet id (QoS > 0) and payload.
func parsePublish(flags byte, body []byte) (string, int, []byte) {
	if len(body) < 2 {
		return "", 0, nil
	}
	tLen := int(binary.BigEndian.Uint16(body[0:2]))
	if 2+tLen > len(body) {
		return "", 0, nil
	}
	topic := string(body[2 : 2+tLen])
	pos := 2 + tLen
	packetID := 0
	if flags&0x06 != 0 && pos+2 <= len(body) {
		packetID = int(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
	}
	return topic, packetID, append([]byte(nil), body[pos:]...)
}

// encodeString prefixes a string with its two-byte length.
func encodeString(s string) []byte {
	out := []byte{byte(len(s) >> 8), byte(len(s))}
	return append(out, s...)
}

// makeServerCert builds a throwaway CA and a server certificate
// signed by it, valid for loopback addresses. Returns the server
// tls.Certificate and the CA PEM (for client verification config).
func makeServerCert() (tls.Certificate, []byte, error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	now := time.Now()
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mqtt test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	srvKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	srvTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	srvCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER})
	srvKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(srvKey)})

	cert, err := tls.X509KeyPair(srvCertPEM, srvKeyPEM)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("server keypair: %w", err)
	}
	return cert, caPEM, nil
}

// Will is the last-will a raw client registers at CONNECT time.
type Will struct {
	Topic   string
	Payload string
	QoS     byte
	Retain  bool
}

// RawConnect dials a plaintext broker and hand-writes a minimal MQTT
// 3.1.1 CONNECT packet, returning the open connection after reading
// the CONNACK. It exists so tests can drop a client ungracefully —
// closing the returned connection without a DISCONNECT packet is what
// triggers the broker's last-will path, something a well-behaved
// client library never does on demand.
func RawConnect(addr, clientID string, will *Will) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	// Variable header: protocol name, level 4 (3.1.1), flags, keepalive;
	// payload: client id plus the will topic/message when present.
	flags := byte(0x02) // clean session
	body := encodeString(clientID)
	if will != nil {
		flags |= 0x04 | (will.QoS&0x03)<<3
		if will.Retain {
			flags |= 0x20
		}
		body = append(body, encodeString(will.Topic)...)
		body = append(body, encodeString(will.Payload)...)
	}
	vh := append(encodeString("MQTT"), 4, flags, 0, 60)
	packet := append(vh, body...)
	if err := writePacket(conn, pktCONNECT, 0, packet); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	typ, _, ack, err := readPacket(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("CONNACK read: %w", err)
	}
	if typ != pktCONNACK || len(ack) < 2 || ack[1] != 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("connect refused (packet %d, %v)", typ, ack)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return conn, nil
}
