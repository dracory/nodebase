package mqttout

// Internal coverage for the mqttOut connection fields added for the
// broker-config parity work — decode plus validation, no broker needed.

import (
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// TestMQTTOutConfigDecode checks every connection field decodes
// through the node factory path (DecodeConfig): clientID, clean
// session, keepalive, TLS and will.
func TestMQTTOutConfigDecode(t *testing.T) {
	node := nodestore.NewNode("n1", "mqttOut").SetData(map[string]interface{}{
		"broker":        "broker-1",
		"topic":         "a/b",
		"retain":        true,
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
	c := &MQTTOutConfig{}
	if err := nodebase.DecodeConfig(node, c); err != nil {
		t.Fatalf("DecodeConfig failed: %v", err)
	}
	if c.Broker != "broker-1" || c.Topic != "a/b" || !c.Retain {
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

// TestMQTTOutValidateTLSPairRule covers the inline cert/key pairing
// check added with the TLS fields.
func TestMQTTOutValidateTLSPairRule(t *testing.T) {
	n := NewMQTTOutNode(nodestore.NewNode("n1", "mqttOut"), &MQTTOutConfig{
		MQTTConnConfig: nodebase.MQTTConnConfig{Broker: "b:1", TLSKey: "/key"},
	}, nil)
	if err := n.Validate(); err == nil {
		t.Fatal("lone tlskey must fail validation")
	}
}

// TestPublishTopicPrecedence pins the Node-RED rule: the configured
// topic wins over msg.topic, which is only the fallback for a blank
// config — otherwise an mqttIn → mqttOut chain republishes onto the
// subscription topic and loops.
func TestPublishTopicPrecedence(t *testing.T) {
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{"topic": "msg/topic"})
	if got := publishTopic("configured/topic", msg); got != "configured/topic" {
		t.Fatalf("configured topic must win over msg.topic, got %q", got)
	}
	if got := publishTopic("", msg); got != "msg/topic" {
		t.Fatalf("blank config must fall back to msg.topic, got %q", got)
	}
	if got := publishTopic("", nodestore.NewMessage()); got != "" {
		t.Fatalf("no config and no msg.topic must stay empty, got %q", got)
	}
}
