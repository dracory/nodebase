package nodebase

// Pack-local unit tests for the shared MQTT connection model:
// MQTTBrokerConfig/MQTTConnConfig decoding, broker-node resolution
// (the config-node seam), TLS materialization and paho dial-option
// construction — all without a live broker.

import (
	"crypto/tls"
	"encoding/json"
	"testing"

	nodestore "github.com/dracory/nodestore"
)

// brokerStubHost extends stubHost with a node table plus the
// credential-preserving LookupNodeForConfig seam the engine
// implements.
type brokerStubHost struct {
	stubHost
	nodes      map[string]nodestore.NodeInterface
	rawLookup  bool
	tlsConfigs map[string]*tls.Config
}

func (h *brokerStubHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) {
	if h.rawLookup {
		return nil, false
	}
	n, ok := h.nodes[nodeID]
	return n, ok
}

func (h *brokerStubHost) LookupNodeForConfig(wf, nodeID string) (nodestore.NodeInterface, bool) {
	if !h.rawLookup {
		return nil, false
	}
	n, ok := h.nodes[nodeID]
	return n, ok
}

func (h *brokerStubHost) LoadTLSConfig(wf, ref string) (*tls.Config, error) {
	cfg, ok := h.tlsConfigs[ref]
	if !ok {
		return nil, nil
	}
	return cfg, nil
}

func brokerNode(data map[string]interface{}) nodestore.NodeInterface {
	return nodestore.NewNode("broker-1", "mqttBroker").SetData(data)
}

// TestMQTTConnConfigDecode verifies every inline connection field
// decodes from node data — clientID, clean session, keepalive, TLS
// and last-will.
func TestMQTTConnConfigDecode(t *testing.T) {
	node := nodestore.NewNode("n1", "mqttIn").SetData(map[string]interface{}{
		"broker":        "ssl://h:8883",
		"clientid":      "client-1",
		"cleansession":  false,
		"keepalive":     float64(45),
		"usetls":        true,
		"tlsinsecure":   true,
		"tlsca":         "/ca.pem",
		"tlscert":       "/cert.pem",
		"tlskey":        "/key.pem",
		"tlsservername": "broker.internal",
		"username":      "u",
		"password":      "p",
		"willtopic":     "status/node",
		"willpayload":   "offline",
		"willqos":       float64(1),
		"willretain":    true,
	})
	c := &MQTTConnConfig{}
	if err := DecodeConfig(node, c); err != nil {
		t.Fatalf("DecodeConfig failed: %v", err)
	}
	if c.Broker != "ssl://h:8883" || c.ClientID != "client-1" || c.KeepAlive != 45 {
		t.Fatalf("connection fields decoded wrong: %+v", c)
	}
	if c.CleanSession == nil || *c.CleanSession != false {
		t.Fatal("cleansession did not decode")
	}
	if !c.UseTLS || !c.TLSInsecure || c.TLSCA != "/ca.pem" || c.TLSCert != "/cert.pem" ||
		c.TLSKey != "/key.pem" || c.TLSServerName != "broker.internal" {
		t.Fatalf("tls fields decoded wrong: %+v", c)
	}
	if c.Username != "u" || c.Password != "p" {
		t.Fatal("credentials did not decode")
	}
	if c.WillTopic != "status/node" || c.WillPayload != "offline" || c.WillQoS != 1 || !c.WillRetain {
		t.Fatalf("will fields decoded wrong: %+v", c)
	}
}

// TestMQTTBrokerConfigDecode verifies the mqtt-broker (Node-RED)
// field spellings decode on the config node.
func TestMQTTBrokerConfigDecode(t *testing.T) {
	node := nodestore.NewNode("b1", "mqttBroker").SetData(map[string]interface{}{
		"broker":           "broker.local",
		"port":             "8883",
		"clientid":         "shared-client",
		"user":             "bu",
		"password":         "bp",
		"cleansession":     false,
		"keepalive":        float64(30),
		"usetls":           true,
		"verifyservercert": false,
		"tls":              "tls-node-1",
		"willtopic":        "status/will",
		"willpayload":      "bye",
		"willqos":          float64(2),
		"willretain":       true,
	})
	c := &MQTTBrokerConfig{}
	if err := DecodeConfig(node, c); err != nil {
		t.Fatalf("DecodeConfig failed: %v", err)
	}
	if c.Broker != "broker.local" || c.Port != "8883" || c.ClientID != "shared-client" {
		t.Fatalf("address/clientid decoded wrong: %+v", c)
	}
	if c.User != "bu" || c.Password != "bp" {
		t.Fatal("credentials did not decode")
	}
	if c.CleanSession == nil || *c.CleanSession != false {
		t.Fatal("cleansession did not decode")
	}
	if c.KeepAlive != 30 || !c.UseTLS {
		t.Fatalf("keepalive/usetls decoded wrong: %+v", c)
	}
	if c.VerifyServerCert == nil || *c.VerifyServerCert != false {
		t.Fatal("verifyservercert did not decode")
	}
	if c.TLS != "tls-node-1" {
		t.Fatal("tls ref did not decode")
	}
	if c.WillTopic != "status/will" || c.WillPayload != "bye" || c.WillQoS != 2 || !c.WillRetain {
		t.Fatalf("will fields decoded wrong: %+v", c)
	}
}

// TestResolveMQTTOptsInline covers the plain-address path: no config
// node, inline fields only.
func TestResolveMQTTOptsInline(t *testing.T) {
	clean := false
	opts, err := ResolveMQTTOpts(&stubHost{}, "wf", &MQTTConnConfig{
		Broker:       "127.0.0.1:1883",
		ClientID:     "c1",
		Username:     "u",
		Password:     "p",
		CleanSession: &clean,
		KeepAlive:    15,
		WillTopic:    "gone",
		WillQoS:      2,
		WillRetain:   true,
	})
	if err != nil {
		t.Fatalf("ResolveMQTTOpts failed: %v", err)
	}
	if opts.Broker != "127.0.0.1:1883" || opts.ClientID != "c1" ||
		opts.Username != "u" || opts.Password != "p" {
		t.Fatalf("opts decoded wrong: %+v", opts)
	}
	if opts.CleanSession || opts.KeepAlive != 15 {
		t.Fatalf("session/keepalive wrong: %+v", opts)
	}
	if opts.WillTopic != "gone" || opts.WillQoS != 2 || !opts.WillRetain {
		t.Fatalf("will wrong: %+v", opts)
	}
	if opts.TLS != nil {
		t.Fatal("plain tcp broker must not get a TLS config")
	}
}

// TestResolveMQTTOptsBrokerNode resolves a broker field that holds an
// mqttBroker node id: config-node fields win, inline fills gaps, the
// host/port pair is merged and real credentials come through the
// LookupNodeForConfig seam.
func TestResolveMQTTOptsBrokerNode(t *testing.T) {
	host := &brokerStubHost{
		rawLookup: true,
		nodes: map[string]nodestore.NodeInterface{
			"broker-1": brokerNode(map[string]interface{}{
				"broker":           "broker.local",
				"port":             "8883",
				"clientid":         "shared",
				"user":             "bu",
				"password":         "bp",
				"cleansession":     false,
				"keepalive":        float64(30),
				"usetls":           true,
				"verifyservercert": false,
				"willtopic":        "status/will",
				"willqos":          float64(1),
			}),
		},
	}
	opts, err := ResolveMQTTOpts(host, "wf", &MQTTConnConfig{
		Broker:      "broker-1",
		Username:    "inline-user",
		WillPayload: "inline-payload",
	})
	if err != nil {
		t.Fatalf("ResolveMQTTOpts failed: %v", err)
	}
	if opts.Broker != "broker.local:8883" {
		t.Fatalf("host+port not merged: %q", opts.Broker)
	}
	if opts.ClientID != "shared" || opts.Username != "bu" || opts.Password != "bp" {
		t.Fatalf("config-node fields must win: %+v", opts)
	}
	if opts.CleanSession {
		t.Fatal("config-node cleansession:false must win")
	}
	if opts.WillTopic != "status/will" || opts.WillPayload != "inline-payload" || opts.WillQoS != 1 {
		t.Fatalf("will merge wrong: %+v", opts)
	}
	if opts.TLS == nil {
		t.Fatal("usetls must produce a TLS config")
	}
	if !opts.TLS.InsecureSkipVerify {
		t.Fatal("verifyservercert:false must set InsecureSkipVerify")
	}
}

// TestResolveMQTTOptsBrokerNodeMaskedCreds covers the degraded path:
// a host without LookupNodeForConfig returns sentinel-masked creds,
// which must be treated as unset (never dialed literally).
func TestResolveMQTTOptsBrokerNodeMaskedCreds(t *testing.T) {
	host := &brokerStubHost{
		rawLookup: false, // LookupNode path only
		nodes: map[string]nodestore.NodeInterface{
			"broker-1": brokerNode(map[string]interface{}{
				"broker":   "broker.local:1883",
				"user":     nodestore.CredentialSentinel,
				"password": nodestore.CredentialSentinel,
			}),
		},
	}
	opts, err := ResolveMQTTOpts(host, "wf", &MQTTConnConfig{
		Broker:   "broker-1",
		Username: "inline-user",
		Password: "inline-pass",
	})
	if err != nil {
		t.Fatalf("ResolveMQTTOpts failed: %v", err)
	}
	if opts.Username != "inline-user" || opts.Password != "inline-pass" {
		t.Fatalf("masked creds must fall back to inline: %+v", opts)
	}
}

// TestResolveMQTTOptsWrongRefType rejects a broker field pointing at a
// non-broker node.
func TestResolveMQTTOptsWrongRefType(t *testing.T) {
	host := &brokerStubHost{
		rawLookup: true,
		nodes: map[string]nodestore.NodeInterface{
			"dbg": nodestore.NewNode("dbg", "debug"),
		},
	}
	if _, err := ResolveMQTTOpts(host, "wf", &MQTTConnConfig{Broker: "dbg"}); err == nil {
		t.Fatal("expected type-mismatch error")
	}
}

// TestResolveMQTTOptsTLSRef resolves TLS material through a tlsConfig
// node reference.
func TestResolveMQTTOptsTLSRef(t *testing.T) {
	host := &brokerStubHost{
		tlsConfigs: map[string]*tls.Config{
			"tls-1": {ServerName: "broker.internal"},
		},
	}
	opts, err := ResolveMQTTOpts(host, "wf", &MQTTConnConfig{
		Broker: "broker.local:8883",
		TLS:    "tls-1",
	})
	if err != nil {
		t.Fatalf("ResolveMQTTOpts failed: %v", err)
	}
	if opts.TLS == nil || opts.TLS.ServerName != "broker.internal" {
		t.Fatalf("tlsConfig material not resolved: %+v", opts.TLS)
	}
}

// TestResolveMQTTOptsSSLScheme covers the ssl:// shorthand: TLS with
// default verification and no explicit fields.
func TestResolveMQTTOptsSSLScheme(t *testing.T) {
	opts, err := ResolveMQTTOpts(&stubHost{}, "wf", &MQTTConnConfig{Broker: "ssl://b:8883"})
	if err != nil {
		t.Fatalf("ResolveMQTTOpts failed: %v", err)
	}
	if opts.TLS == nil || opts.TLS.InsecureSkipVerify {
		t.Fatalf("ssl:// must enable verified TLS: %+v", opts.TLS)
	}
}

// TestResolveMQTTOptsInlineTLSPairRule rejects a lone cert without key.
func TestResolveMQTTOptsInlineTLSPairRule(t *testing.T) {
	_, err := ResolveMQTTOpts(&stubHost{}, "wf", &MQTTConnConfig{
		Broker:  "b:1",
		TLSCert: "/cert.pem",
	})
	if err == nil {
		t.Fatal("expected cert/key pairing error")
	}
}

// TestMQTTClientOptions asserts the paho dial options built from
// resolved options — the dial-side half of the TLS/will/session work.
func TestMQTTClientOptions(t *testing.T) {
	opts := MQTTClientOptions(&MQTTOpts{
		Broker:       "b:1883",
		Username:     "u",
		Password:     "p",
		CleanSession: false,
		KeepAlive:    20,
		TLS:          &tls.Config{InsecureSkipVerify: true},
		WillTopic:    "w/topic",
		WillPayload:  "w/payload",
		WillQoS:      2,
		WillRetain:   true,
	}, "cid-default")
	if opts.ClientID != "cid-default" {
		t.Fatalf("clientID fallback wrong: %q", opts.ClientID)
	}
	if !opts.WillEnabled || opts.WillTopic != "w/topic" ||
		string(opts.WillPayload) != "w/payload" || opts.WillQos != 2 || !opts.WillRetained {
		t.Fatalf("will options wrong: %+v", opts)
	}
	if opts.CleanSession {
		t.Fatal("cleanSession must propagate")
	}
	if opts.KeepAlive != 20 {
		t.Fatalf("keepAlive wrong: %d", opts.KeepAlive)
	}
	if opts.TLSConfig == nil || !opts.TLSConfig.InsecureSkipVerify {
		t.Fatal("TLSConfig not propagated")
	}
	if opts.Username != "u" || opts.Password != "p" {
		t.Fatal("credentials not propagated")
	}
	if len(opts.Servers) != 1 || opts.Servers[0].String() != "tcp://b:1883" {
		t.Fatalf("broker URL not normalized: %+v", opts.Servers)
	}
}

// TestMQTTConnectOptsClientIDOverride checks a configured clientID
// overrides the caller default.
func TestMQTTConnectOptsClientIDOverride(t *testing.T) {
	opts := MQTTClientOptions(&MQTTOpts{Broker: "b:1", ClientID: "configured", CleanSession: true}, "default")
	if opts.ClientID != "configured" {
		t.Fatalf("clientID override wrong: %q", opts.ClientID)
	}
}

// TestBrokerAddress covers host/port merging.
func TestBrokerAddress(t *testing.T) {
	if got := brokerAddress("host", "1883"); got != "host:1883" {
		t.Fatalf("merge failed: %q", got)
	}
	if got := brokerAddress("host:1883", "9999"); got != "host:1883" {
		t.Fatalf("explicit port must win: %q", got)
	}
	if got := brokerAddress("ssl://host", "8883"); got != "ssl://host" {
		t.Fatalf("scheme host must pass through: %q", got)
	}
}

// TestMQTTBrokerConfigJSONRoundTrip pins the Node-RED field spellings
// so a future rename cannot silently break import compatibility.
func TestMQTTBrokerConfigJSONRoundTrip(t *testing.T) {
	raw, err := json.Marshal(&MQTTBrokerConfig{Broker: "b", Port: "1883", ClientID: "c", User: "u"})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"broker", "port", "clientid", "user"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("field %q missing from JSON: %v", k, m)
		}
	}
}
