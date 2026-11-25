package mqttout

// Internal coverage for the mqttOut connection fields added for the
// broker-config parity work — decode plus validation, no broker needed.

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	"github.com/dracory/nodebase/mqtttestbroker"
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
	if c.Broker != "broker-1" || c.Topic != "a/b" || c.Retain == nil || !*c.Retain {
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

// TestMQTTOutConfigDecodeBlankVsExplicit pins the Node-RED
// serialization rule: exports write blank qos/retain as "" (must
// decode to nil = unset) while explicit "0"/"false" must decode to
// non-nil pointers — only nil triggers the msg.* fallback.
func TestMQTTOutConfigDecodeBlankVsExplicit(t *testing.T) {
	decode := func(data map[string]interface{}) *MQTTOutConfig {
		t.Helper()
		data["broker"] = "b:1"
		c := &MQTTOutConfig{}
		if err := nodebase.DecodeConfig(nodestore.NewNode("n1", "mqttOut").SetData(data), c); err != nil {
			t.Fatalf("DecodeConfig failed: %v", err)
		}
		return c
	}

	// Node-RED export shape: blank qos/retain serialize as "".
	c := decode(map[string]interface{}{"qos": "", "retain": ""})
	if c.QoS != nil || c.Retain != nil {
		t.Fatalf("blank qos/retain must decode nil, got %+v", c)
	}

	// Missing keys are blank too.
	c = decode(map[string]interface{}{})
	if c.QoS != nil || c.Retain != nil {
		t.Fatalf("absent qos/retain must decode nil, got %+v", c)
	}

	// Explicit values — including the zero values "0"/"false" — are
	// configured, not blank.
	c = decode(map[string]interface{}{"qos": "0", "retain": "false"})
	if c.QoS == nil || *c.QoS != 0 || c.Retain == nil || *c.Retain != false {
		t.Fatalf("explicit 0/false must decode non-nil, got %+v", c)
	}
	c = decode(map[string]interface{}{"qos": "2", "retain": "true"})
	if c.QoS == nil || *c.QoS != 2 || c.Retain == nil || *c.Retain != true {
		t.Fatalf("explicit 2/true must decode, got %+v", c)
	}
}

// TestPublishQoSPrecedence pins the Node-RED rule: a configured qos
// wins over msg.qos, which is only the fallback for a blank config.
func TestPublishQoSPrecedence(t *testing.T) {
	two := 2
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{"qos": 1})
	if got := publishQoS(&two, msg); got != 2 {
		t.Fatalf("configured qos must win over msg.qos, got %d", got)
	}
	zero := 0
	if got := publishQoS(&zero, msg); got != 0 {
		t.Fatalf("explicit qos 0 must win over msg.qos, got %d", got)
	}
	if got := publishQoS(nil, msg); got != 1 {
		t.Fatalf("blank config must fall back to msg.qos, got %d", got)
	}
	// String metadata coerces the way Node-RED accepts msg.qos strings.
	strMsg := nodestore.NewMessage().SetMetadata(map[string]interface{}{"qos": "2"})
	if got := publishQoS(nil, strMsg); got != 2 {
		t.Fatalf("string msg.qos must coerce, got %d", got)
	}
	if got := publishQoS(nil, nodestore.NewMessage()); got != 0 {
		t.Fatalf("no config and no msg.qos must default 0, got %d", got)
	}
}

// TestPublishRetainPrecedence pins the same rule for retain: the
// configured flag — including explicit false — wins; msg.retain is
// only the fallback for a blank config.
func TestPublishRetainPrecedence(t *testing.T) {
	tr, fa := true, false
	msg := nodestore.NewMessage().SetMetadata(map[string]interface{}{"retain": true})
	if !publishRetain(&tr, msg) {
		t.Fatal("configured retain=true must win over msg.retain")
	}
	if publishRetain(&fa, msg) {
		t.Fatal("explicit retain=false must win over msg.retain")
	}
	if !publishRetain(nil, msg) {
		t.Fatal("blank config must fall back to msg.retain")
	}
	if publishRetain(nil, nodestore.NewMessage()) {
		t.Fatal("no config and no msg.retain must default false")
	}
}

// fakeHost keeps node state so Execute can build the shared client
// pool — the minimal NodeHost for the live-broker tests.
type fakeHost struct {
	mu    sync.Mutex
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

// TestMQTTOutMsgPropsFallbackLive decodes a Node-RED-export-shaped
// mqttOut node (blank qos/retain serialized as "") and publishes two
// messages against the in-process test broker: the wire flags must
// come from msg.qos/msg.retain metadata — the fallback Node-RED
// applies when the config is blank — while an explicit config wins.
func TestMQTTOutMsgPropsFallbackLive(t *testing.T) {
	broker, err := mqtttestbroker.Start()
	if err != nil {
		t.Fatalf("start broker: %v", err)
	}
	defer broker.Close()

	node := nodestore.NewNode("n1", "mqttOut").SetWorkflowID("wf").SetData(map[string]interface{}{
		"broker": broker.Addr(),
		"topic":  "props/fallback",
		"qos":    "", // Node-RED blank serialization — msg.qos fallback
		"retain": "",
	})
	cfg := &MQTTOutConfig{}
	if err := nodebase.DecodeConfig(node, cfg); err != nil {
		t.Fatalf("DecodeConfig failed: %v", err)
	}
	n := NewMQTTOutNode(node, cfg, &fakeHost{})

	publish := func(meta map[string]interface{}) {
		t.Helper()
		msg := nodestore.NewMessage().SetPayload("hi").SetMetadata(meta)
		if _, err := n.Execute(context.Background(), msg); err != nil {
			t.Fatalf("execute failed: %v", err)
		}
	}
	publish(map[string]interface{}{"qos": 1, "retain": true})
	publish(map[string]interface{}{"qos": 0, "retain": false})

	deadline := time.Now().Add(5 * time.Second)
	for len(broker.PublishedMessages()) < 2 && time.Now().Before(deadline) {
		time.Sleep(15 * time.Millisecond)
	}
	if len(broker.PublishedMessages()) < 2 {
		t.Fatalf("broker saw %d publishes, want 2", len(broker.PublishedMessages()))
	}
	if got := broker.PublishedMessages()[0]; got.QoS != 1 || !got.Retain {
		t.Fatalf("msg-driven flags not honored: %+v", got)
	}
	if got := broker.PublishedMessages()[1]; got.QoS != 0 || got.Retain {
		t.Fatalf("msg-driven flags not honored: %+v", got)
	}

	// Explicit config wins over message metadata on the same wire.
	one, tr := 1, true
	cfg.QoS, cfg.Retain = &one, &tr
	publish(map[string]interface{}{"qos": 0, "retain": false})
	for len(broker.PublishedMessages()) < 3 && time.Now().Before(deadline) {
		time.Sleep(15 * time.Millisecond)
	}
	if len(broker.PublishedMessages()) < 3 {
		t.Fatalf("broker saw %d publishes, want 3", len(broker.PublishedMessages()))
	}
	if got := broker.PublishedMessages()[2]; got.QoS != 1 || !got.Retain {
		t.Fatalf("configured flags must win over msg metadata: %+v", got)
	}
}
