package nodebase

import (
	"errors"
	"fmt"
	"sync"

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

// Client returns a connected client for the resolved dial options,
// connecting and caching it on first use. The pool key includes the
// username, client id and a TLS marker so nodes against the same
// broker with different credentials, identities or transports get
// separate connections.
func (p *MQTTClientPool) Client(opts *MQTTOpts, clientID string) (mqtt.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := opts.Broker + "\x00" + opts.Username + "\x00" + firstNonEmpty(opts.ClientID, clientID) + "\x00" + fmt.Sprint(opts.TLS != nil)
	if c, ok := p.clients[key]; ok && c.IsConnected() {
		return c, nil
	}
	c, err := MQTTConnectOpts(opts, clientID)
	if err != nil {
		return nil, err
	}
	p.clients[key] = c
	return c, nil
}

// MQTTConnect builds a paho client for the given broker (host:port is
// normalized to tcp://host:port) and connects, surfacing broker
// unavailability as an error. Kept as the plain-address convenience
// form; richer connections go through MQTTConnectOpts.
func MQTTConnect(broker, clientID, username, password string) (mqtt.Client, error) {
	return MQTTConnectOpts(&MQTTOpts{
		Broker:       broker,
		Username:     username,
		Password:     password,
		CleanSession: true,
	}, clientID)
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
	raw, err := PayloadBytes(input.GetPayload())
	if err != nil {
		return nil, fmt.Errorf("mqttOut cannot serialize data: %w", err)
	}
	return raw, nil
}

// NewMQTTClientPool returns an empty pool (the value stored under
// MQTTClientsKey in host node state).
func NewMQTTClientPool() *MQTTClientPool {
	return &MQTTClientPool{clients: map[string]mqtt.Client{}}
}
