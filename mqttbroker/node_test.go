package mqttbroker

// Internal coverage for the mqttBroker config node: registration,
// decode through the factory and structural validation.

import (
	"testing"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// TestMQTTBrokerRegistered checks the type is registered under the
// engine id the import mapping produces.
func TestMQTTBrokerRegistered(t *testing.T) {
	if _, ok := nodebase.RegisteredNode(nodebase.MQTTBrokerType); !ok {
		t.Fatal("mqttBroker not registered")
	}
}

// TestMQTTBrokerFactoryDecode exercises the factory decode path with
// the Node-RED field spellings.
func TestMQTTBrokerFactoryDecode(t *testing.T) {
	node := nodestore.NewNode("b1", "mqttBroker").SetData(map[string]interface{}{
		"broker":           "broker.local",
		"port":             "8883",
		"clientid":         "shared",
		"user":             "u",
		"password":         "p",
		"cleansession":     false,
		"keepalive":        float64(30),
		"usetls":           true,
		"verifyservercert": false,
		"willtopic":        "w/t",
		"willpayload":      "w/p",
		"willqos":          float64(1),
		"willretain":       true,
	})
	ex, err := factory(node, nil)
	if err != nil {
		t.Fatalf("factory failed: %v", err)
	}
	if err := ex.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

// TestMQTTBrokerValidate covers the structural rules: broker required,
// inline cert/key paired.
func TestMQTTBrokerValidate(t *testing.T) {
	n := NewMQTTBrokerNode(nodestore.NewNode("b1", "mqttBroker"), &nodebase.MQTTBrokerConfig{})
	if err := n.Validate(); err == nil {
		t.Fatal("empty broker must fail validation")
	}
	n2 := NewMQTTBrokerNode(nodestore.NewNode("b2", "mqttBroker"), &nodebase.MQTTBrokerConfig{
		Broker:  "b:1",
		TLSCert: "/cert",
	})
	if err := n2.Validate(); err == nil {
		t.Fatal("lone tlscert must fail validation")
	}
}
