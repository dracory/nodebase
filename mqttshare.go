package nodebase

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	nodestore "github.com/dracory/nodestore"
)

// MQTTClientPool is the workflow-level registry of shared broker
// connections keyed by broker address, so mqttIn/mqttOut nodes against
// the same broker share one TCP connection. Stored in host node state
// under MQTTClientsKey — rebuilt per workflow run.
type MQTTClientPool struct {
	mu      sync.Mutex
	clients map[string]mqtt.Client
}

const MQTTClientsKey = "__mqttClients__"

// client returns a connected client for broker, connecting and caching
// it on first use. The pool key includes the username so nodes against
// the same broker with different credentials get separate connections.
func (p *MQTTClientPool) Client(broker, clientID, username, password string) (mqtt.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := broker + "\x00" + username
	if c, ok := p.clients[key]; ok && c.IsConnected() {
		return c, nil
	}
	c, err := MQTTConnect(broker, clientID, username, password)
	if err != nil {
		return nil, err
	}
	p.clients[key] = c
	return c, nil
}

// MQTTConnect builds a paho client for the given broker (host:port is
// normalized to tcp://host:port) and connects, surfacing broker
// unavailability as an error.
func MQTTConnect(broker, clientID, username, password string) (mqtt.Client, error) {
	addr := strings.TrimSpace(broker)
	if addr == "" {
		return nil, errors.New("mqtt broker address is empty")
	}
	if !strings.Contains(addr, "://") {
		addr = "tcp://" + addr
	}
	opts := mqtt.NewClientOptions().
		AddBroker(addr).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetConnectTimeout(10 * time.Second)
	if username != "" {
		opts.SetUsername(username).SetPassword(password)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(12 * time.Second) {
		return nil, fmt.Errorf("mqtt connect to %s timed out", addr)
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt connect to %s failed: %w", addr, err)
	}
	return client, nil
}

// ClampQoS constrains QoS to the MQTT range 0-2.
func ClampQoS(qos int) int {
	if qos < 0 {
		return 0
	}
	if qos > 2 {
		return 2
	}
	return qos
}

// MQTTPayloadBytes converts message Data into publish bytes: strings and
// []byte pass through, anything else is JSON-marshaled.
func MQTTPayloadBytes(input nodestore.MessageInterface) ([]byte, error) {
	if input == nil || input.GetPayload() == nil {
		return nil, errors.New("mqttOut requires message data")
	}
	switch v := input.GetPayload().(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("mqttOut cannot serialize data: %w", err)
		}
		return raw, nil
	}
}

// NewMQTTClientPool returns an empty pool (the value stored under
// MQTTClientsKey in host node state).
func NewMQTTClientPool() *MQTTClientPool {
	return &MQTTClientPool{clients: map[string]mqtt.Client{}}
}
