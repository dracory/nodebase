package nodebase

// Live-broker tests for the MQTT dial path — an in-process minimal
// broker (mqtttestbroker) exercises TLS verification, the insecure
// opt-out, plaintext round trips and last-will delivery with real
// paho clients, no external broker needed.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/dracory/nodebase/mqtttestbroker"
)

// writeCAPEM writes the broker's CA bundle into the test temp dir and
// returns its path — exercising the same pem-path config real flows use.
func writeCAPEM(t *testing.T, b *mqtttestbroker.Broker) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, b.CAPEM(), 0o600); err != nil {
		t.Fatalf("write ca.pem: %v", err)
	}
	return path
}

// roundTrip subscribes and publishes over an established client,
// failing the test unless the message comes back.
func roundTrip(t *testing.T, client mqtt.Client, topic string) {
	t.Helper()
	got := make(chan string, 1)
	token := client.Subscribe(topic, 0, func(_ mqtt.Client, m mqtt.Message) {
		got <- string(m.Payload())
	})
	if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe failed: %v", token.Error())
	}
	pub := client.Publish(topic, 0, false, "hello")
	if !pub.WaitTimeout(5*time.Second) || pub.Error() != nil {
		t.Fatalf("publish failed: %v", pub.Error())
	}
	select {
	case m := <-got:
		if m != "hello" {
			t.Fatalf("round trip payload = %q", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("round trip timed out — message never came back")
	}
}

// TestMQTTTLSConnectCAVerified dials a TLS broker with usetls on a
// bare host:port address, verifying the broker certificate against
// the test CA configured via the tlsca pem path. This is the exact
// shape Node-RED flows use: host:port + usetls + CA bundle.
func TestMQTTTLSConnectCAVerified(t *testing.T) {
	broker, err := mqtttestbroker.StartTLS()
	if err != nil {
		t.Fatalf("start TLS broker: %v", err)
	}
	defer broker.Close()

	conn := &MQTTConnConfig{
		Broker: broker.Addr(), // bare host:port — no scheme
		UseTLS: true,
		TLSCA:  writeCAPEM(t, broker),
	}
	opts, err := ResolveMQTTOpts(nil, "wf", conn)
	if err != nil {
		t.Fatalf("ResolveMQTTOpts: %v", err)
	}
	if opts.TLS == nil {
		t.Fatal("usetls should materialize a tls.Config")
	}
	client, err := MQTTConnectOpts(opts, "tls-test-client")
	if err != nil {
		t.Fatalf("TLS connect with CA verification failed: %v", err)
	}
	defer client.Disconnect(250)
	roundTrip(t, client, "tls/roundtrip")
}

// TestMQTTTLSInsecureSkipVerify connects without any CA material
// using the tlsinsecure opt-out — verification is skipped and the
// self-signed broker certificate is accepted.
func TestMQTTTLSInsecureSkipVerify(t *testing.T) {
	broker, err := mqtttestbroker.StartTLS()
	if err != nil {
		t.Fatalf("start TLS broker: %v", err)
	}
	defer broker.Close()

	conn := &MQTTConnConfig{
		Broker:      broker.Addr(),
		UseTLS:      true,
		TLSInsecure: true,
	}
	opts, err := ResolveMQTTOpts(nil, "wf", conn)
	if err != nil {
		t.Fatalf("ResolveMQTTOpts: %v", err)
	}
	client, err := MQTTConnectOpts(opts, "tls-insecure-client")
	if err != nil {
		t.Fatalf("TLS connect with skip-verify failed: %v", err)
	}
	defer client.Disconnect(250)
	roundTrip(t, client, "tls/insecure")
}

// TestMQTTTLSVerificationFailsWithoutCA proves verification is real:
// with usetls but neither CA material nor the insecure opt-out, the
// self-signed broker certificate must be rejected.
func TestMQTTTLSVerificationFailsWithoutCA(t *testing.T) {
	broker, err := mqtttestbroker.StartTLS()
	if err != nil {
		t.Fatalf("start TLS broker: %v", err)
	}
	defer broker.Close()

	conn := &MQTTConnConfig{
		Broker: broker.Addr(),
		UseTLS: true,
	}
	opts, err := ResolveMQTTOpts(nil, "wf", conn)
	if err != nil {
		t.Fatalf("ResolveMQTTOpts: %v", err)
	}
	if _, err := MQTTConnectOpts(opts, "tls-reject-client"); err == nil {
		t.Fatal("connect without CA or skip-verify must fail certificate verification")
	}
}

// TestMQTTRoundTripPlaintext exercises the plain TCP dial path.
func TestMQTTRoundTripPlaintext(t *testing.T) {
	broker, err := mqtttestbroker.Start()
	if err != nil {
		t.Fatalf("start broker: %v", err)
	}
	defer broker.Close()

	conn := &MQTTConnConfig{Broker: broker.Addr()}
	opts, err := ResolveMQTTOpts(nil, "wf", conn)
	if err != nil {
		t.Fatalf("ResolveMQTTOpts: %v", err)
	}
	if opts.TLS != nil {
		t.Fatal("plaintext broker must not materialize a tls.Config")
	}
	client, err := MQTTConnectOpts(opts, "plain-test-client")
	if err != nil {
		t.Fatalf("plaintext connect failed: %v", err)
	}
	defer client.Disconnect(250)
	roundTrip(t, client, "plain/roundtrip")
}

// TestMQTTWillDeliveredOnDrop verifies last-will handling end to end:
// a raw client registers a will at CONNECT time and drops ungracefully;
// the subscribed paho client must receive the will message.
func TestMQTTWillDeliveredOnDrop(t *testing.T) {
	broker, err := mqtttestbroker.Start()
	if err != nil {
		t.Fatalf("start broker: %v", err)
	}
	defer broker.Close()

	// Subscriber via the real dial path, watching the will topic.
	opts, err := ResolveMQTTOpts(nil, "wf", &MQTTConnConfig{Broker: broker.Addr()})
	if err != nil {
		t.Fatalf("ResolveMQTTOpts: %v", err)
	}
	sub, err := MQTTConnectOpts(opts, "will-watcher")
	if err != nil {
		t.Fatalf("connect subscriber: %v", err)
	}
	defer sub.Disconnect(250)
	got := make(chan string, 1)
	token := sub.Subscribe("status/#", 0, func(_ mqtt.Client, m mqtt.Message) {
		got <- m.Topic() + "=" + string(m.Payload())
	})
	if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe failed: %v", token.Error())
	}

	// Raw client: CONNECT with a will, then drop without DISCONNECT.
	raw, err := mqtttestbroker.RawConnect(broker.Addr(), "dying-client", &mqtttestbroker.Will{
		Topic:   "status/dying-client",
		Payload: "offline",
	})
	if err != nil {
		t.Fatalf("raw connect: %v", err)
	}
	_ = raw.Close() // ungraceful drop — the broker must publish the will

	select {
	case m := <-got:
		if m != "status/dying-client=offline" {
			t.Fatalf("will message = %q", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("will never delivered after ungraceful drop")
	}
}
