package mqttin

// Internal coverage ported from flowrunner's node_mqtt_test.go
// (subscriptionTopic is unexported on MQTTInConfig).

import (
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// TestMQTTSharedSubscriptionTopic checks the effective filter built from
// topic + shareGroup without needing a broker.
func TestMQTTSharedSubscriptionTopic(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		group string
		want  string
	}{
		{"plain", "a/b", "", "a/b"},
		{"shared", "a/b", "workers", "$share/workers/a/b"},
		{"shared wildcard", "a/#", "g", "$share/g/a/#"},
		{"raw share passthrough", "$share/g/a/b", "", "$share/g/a/b"},
		{"raw share wins over group", "$share/g/a/b", "other", "$share/g/a/b"},
		{"group whitespace trimmed", "a/b", " g ", "$share/g/a/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &MQTTInConfig{Topic: tc.topic, ShareGroup: tc.group}
			if got := c.subscriptionTopic(); got != tc.want {
				t.Fatalf("subscriptionTopic()=%q want %q", got, tc.want)
			}
		})
	}
}

// TestMQTTInConfigDecode checks every connection field added for the
// broker-config parity work decodes through the node factory path
// (DecodeConfig): clientID, clean session, keepalive, TLS and will.
func TestMQTTInConfigDecode(t *testing.T) {
	node := nodestore.NewNode("n1", "mqttIn").SetData(map[string]interface{}{
		"broker":        "broker-1",
		"topic":         "a/#",
		"clientid":      "cid",
		"cleansession":  false,
		"keepalive":     float64(60),
		"usetls":        true,
		"tlsinsecure":   true,
		"tls":           "tls-1",
		"tlsca":         "/ca",
		"tlscert":       "/cert",
		"tlskey":        "/key",
		"tlsservername": "sni",
		"username":      "u",
		"password":      "p",
		"willtopic":     "w/t",
		"willpayload":   "w/p",
		"willqos":       float64(2),
		"willretain":    true,
	})
	c := &MQTTInConfig{}
	if err := nodebase.DecodeConfig(node, c); err != nil {
		t.Fatalf("DecodeConfig failed: %v", err)
	}
	if c.Broker != "broker-1" || c.Topic != "a/#" {
		t.Fatalf("base fields wrong: %+v", c)
	}
	if c.ClientID != "cid" || c.CleanSession == nil || *c.CleanSession != false || c.KeepAlive != 60 {
		t.Fatalf("session fields wrong: %+v", c)
	}
	if !c.UseTLS || !c.TLSInsecure || c.TLS != "tls-1" || c.TLSCA != "/ca" ||
		c.TLSCert != "/cert" || c.TLSKey != "/key" || c.TLSServerName != "sni" {
		t.Fatalf("tls fields wrong: %+v", c)
	}
	if c.Username != "u" || c.Password != "p" {
		t.Fatal("credentials wrong")
	}
	if c.WillTopic != "w/t" || c.WillPayload != "w/p" || c.WillQoS != 2 || !c.WillRetain {
		t.Fatalf("will fields wrong: %+v", c)
	}
}

// TestMQTTInValidateTLSPairRule covers the inline cert/key pairing
// check added with the TLS fields.
func TestMQTTInValidateTLSPairRule(t *testing.T) {
	n := NewMQTTInNode(nodestore.NewNode("n1", "mqttIn"), &MQTTInConfig{
		MQTTConnConfig: nodebase.MQTTConnConfig{Broker: "b:1", TLSCert: "/cert"},
		Topic:          "a/#",
	}, nil)
	if err := n.Validate(); err == nil {
		t.Fatal("lone tlscert must fail validation")
	}
}
