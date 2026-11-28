package nodebase

// This file holds the shared MQTT broker configuration model — the
// Node-RED `mqtt-broker` config node equivalent plus the inline
// connection fields mqttIn/mqttOut carry, and the resolution that
// merges a broker node reference into dial options.

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	nodestore "github.com/dracory/nodestore"
)

// MQTTBrokerType is the registered type id of the shared broker config
// node (Node-RED `mqtt-broker`).
const MQTTBrokerType = "mqttBroker"

// MQTTBrokerConfig holds configuration for the mqttBroker config node —
// a shared connection description referenced by mqttIn/mqttOut nodes via
// their "broker" field (which then holds the config node's id instead of
// an address). Field spellings follow the Node-RED export format.
type MQTTBrokerConfig struct {
	// Broker is the broker host, host:port or a URL
	// (tcp://, ssl://, ws://, wss://). Port may also be supplied
	// separately in Port (Node-RED keeps them as two fields).
	Broker string `json:"broker"`
	// Port is the optional separate broker port; applied when Broker
	// carries no :port suffix and no URL scheme.
	Port string `json:"port"`
	// ClientID is the MQTT client identifier; empty lets the library
	// generate one per connect.
	ClientID string `json:"clientid"`
	// User/Password are optional broker credentials (the Node-RED
	// mqtt-broker field names). Both are credential-typed keys.
	User     string `json:"user"`
	Password string `json:"password"`
	// CleanSession requests a fresh broker session (default true —
	// Node-RED field cleansession). Pointer so import can distinguish
	// "unset" from an explicit false.
	CleanSession *bool `json:"cleansession"`
	// KeepAlive is the MQTT keepalive in seconds (0 → library default).
	KeepAlive int `json:"keepalive"`
	// UseTLS forces TLS even for a tcp:// address (Node-RED usetls).
	UseTLS bool `json:"usetls"`
	// VerifyServerCert enables peer verification (default true —
	// Node-RED verifyservercert). Pointer for the same unset/false
	// distinction as CleanSession.
	VerifyServerCert *bool `json:"verifyservercert"`
	// TLS references a tlsConfig node id for shared certificate
	// material; takes precedence over the inline tlsca/tlscert/tlskey
	// fields below.
	TLS string `json:"tls"`
	// TLSCA/TLSCert/TLSKey are inline PEM paths used when no tlsConfig
	// reference is configured.
	TLSCA   string `json:"tlsca"`
	TLSCert string `json:"tlscert"`
	TLSKey  string `json:"tlskey"`
	// TLSServerName overrides the verification hostname.
	TLSServerName string `json:"tlsservername"`
	// Will* describe the MQTT last-will published by the broker when
	// this client disconnects ungracefully.
	WillTopic   string `json:"willtopic"`
	WillPayload string `json:"willpayload"`
	WillQoS     int    `json:"willqos"`
	WillRetain  bool   `json:"willretain"`
}

// MQTTConnConfig is the inline connection block embedded in the
// mqttIn/mqttOut node configs. Broker holds either an address
// (host:port or URL) or the id of an mqttBroker config node; in the
// latter case the config node's fields win, with these inline fields
// used only where the config node leaves them empty.
type MQTTConnConfig struct {
	// Broker is host:port, a full URL, or an mqttBroker node id.
	Broker string `json:"broker"`
	// ClientID is the MQTT client identifier.
	ClientID string `json:"clientid"`
	// CleanSession requests a fresh broker session (default true).
	CleanSession *bool `json:"cleansession"`
	// KeepAlive is the MQTT keepalive in seconds.
	KeepAlive int `json:"keepalive"`
	// UseTLS forces TLS even for a tcp:// address.
	UseTLS bool `json:"usetls"`
	// TLSInsecure disables peer verification (insecure opt-out).
	TLSInsecure bool `json:"tlsinsecure"`
	// TLS references a tlsConfig node id for shared certificate
	// material.
	TLS string `json:"tls"`
	// TLSCA/TLSCert/TLSKey are inline PEM paths used when no tlsConfig
	// reference is configured.
	TLSCA   string `json:"tlsca"`
	TLSCert string `json:"tlscert"`
	TLSKey  string `json:"tlskey"`
	// TLSServerName overrides the verification hostname.
	TLSServerName string `json:"tlsservername"`
	// Username/Password are optional broker credentials. They are
	// credential-typed fields: stored outside the exported flow
	// document and merged back in at deploy time.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	// Will* describe the MQTT last-will.
	WillTopic   string `json:"willtopic"`
	WillPayload string `json:"willpayload"`
	WillQoS     int    `json:"willqos"`
	WillRetain  bool   `json:"willretain"`
}

// MQTTOpts is the resolved dial configuration — an address plus every
// connection option a real flow needs.
type MQTTOpts struct {
	// Broker is the normalized address: host:port or scheme URL.
	Broker string
	// ClientID is the MQTT client identifier (empty → generated).
	ClientID     string
	Username     string
	Password     string
	CleanSession bool
	// KeepAlive seconds; 0 → library default.
	KeepAlive int
	// TLS is the transport config; nil for plain TCP.
	TLS *tls.Config
	// Will* describe the last-will; WillTopic empty → no will.
	WillTopic   string
	WillPayload string
	WillQoS     int
	WillRetain  bool
}

// configNodeLookupper is the optional host seam used to read a
// referenced config node including its injected credential values.
// NodeHost.LookupNode deliberately returns a credential-masked view for
// admin surfaces; hosts that want broker credentials to reach the dial
// implement this extra method (the engine does). Hosts without it fall
// back to LookupNode and sentinel-masked credentials are treated as
// unset.
type configNodeLookupper interface {
	LookupNodeForConfig(workflowID, nodeID string) (nodestore.NodeInterface, bool)
}

// lookupConfigNode resolves ref through the credential-preserving host
// seam when available, else through NodeHost.LookupNode.
func lookupConfigNode(host NodeHost, workflowID, ref string) (nodestore.NodeInterface, bool) {
	if host == nil {
		return nil, false
	}
	if l, ok := host.(configNodeLookupper); ok {
		return l.LookupNodeForConfig(workflowID, ref)
	}
	return host.LookupNode(workflowID, ref)
}

// unmaskCredential converts a credential sentinel back to empty so a
// masked value is never dialed as a literal password.
func unmaskCredential(v string) string {
	if nodestore.IsCredentialSentinel(v) {
		return ""
	}
	return v
}

// ResolveMQTTOpts merges an mqtt node's inline connection block with a
// referenced mqttBroker config node (when Broker holds a node id) and
// materializes the TLS material, returning ready dial options. When
// Broker is an address it is used verbatim and only the inline fields
// apply — the previous single-node behaviour.
func ResolveMQTTOpts(host NodeHost, workflowID string, c *MQTTConnConfig) (*MQTTOpts, error) {
	out := &MQTTOpts{
		CleanSession: true,
		WillQoS:      c.WillQoS,
	}

	brokerNode, isBrokerRef := lookupConfigNode(host, workflowID, strings.TrimSpace(c.Broker))
	if isBrokerRef {
		if brokerNode == nil {
			return nil, fmt.Errorf("%s node %q not found", MQTTBrokerType, c.Broker)
		}
		if brokerNode.GetType() != MQTTBrokerType {
			return nil, fmt.Errorf("node %q is type %q, not %s", c.Broker, brokerNode.GetType(), MQTTBrokerType)
		}
		raw, err := json.Marshal(brokerNode.GetData())
		if err != nil {
			return nil, fmt.Errorf("invalid %s data: %w", MQTTBrokerType, err)
		}
		bc := &MQTTBrokerConfig{}
		if err := json.Unmarshal(raw, bc); err != nil {
			return nil, fmt.Errorf("invalid %s data: %w", MQTTBrokerType, err)
		}
		out.Broker = brokerAddress(bc.Broker, bc.Port)
		// The config node's fields win; inline values fill gaps.
		out.ClientID = firstNonEmpty(bc.ClientID, c.ClientID)
		out.Username = firstNonEmpty(unmaskCredential(bc.User), c.Username)
		out.Password = firstNonEmpty(unmaskCredential(bc.Password), c.Password)
		out.KeepAlive = firstNonZero(bc.KeepAlive, c.KeepAlive)
		out.WillTopic = firstNonEmpty(bc.WillTopic, c.WillTopic)
		out.WillPayload = firstNonEmpty(bc.WillPayload, c.WillPayload)
		out.WillQoS = firstNonZero(bc.WillQoS, c.WillQoS)
		out.WillRetain = bc.WillRetain || c.WillRetain
		if bc.CleanSession != nil {
			out.CleanSession = *bc.CleanSession
		} else if c.CleanSession != nil {
			out.CleanSession = *c.CleanSession
		}
		useTLS := bc.UseTLS || c.UseTLS
		insecure := c.TLSInsecure
		if bc.VerifyServerCert != nil && !*bc.VerifyServerCert {
			insecure = true
		}
		cfg, err := resolveTLS(host, workflowID, firstNonEmpty(bc.TLS, c.TLS), tlsInline{
			ca:         firstNonEmpty(bc.TLSCA, c.TLSCA),
			cert:       firstNonEmpty(bc.TLSCert, c.TLSCert),
			key:        firstNonEmpty(bc.TLSKey, c.TLSKey),
			serverName: firstNonEmpty(bc.TLSServerName, c.TLSServerName),
			insecure:   insecure,
			enabled:    useTLS,
		}, out.Broker)
		if err != nil {
			return nil, err
		}
		out.TLS = cfg
	} else {
		out.Broker = strings.TrimSpace(c.Broker)
		out.ClientID = c.ClientID
		out.Username = c.Username
		out.Password = c.Password
		out.KeepAlive = c.KeepAlive
		out.WillTopic = c.WillTopic
		out.WillPayload = c.WillPayload
		out.WillRetain = c.WillRetain
		if c.CleanSession != nil {
			out.CleanSession = *c.CleanSession
		}
		cfg, err := resolveTLS(host, workflowID, c.TLS, tlsInline{
			ca:         c.TLSCA,
			cert:       c.TLSCert,
			key:        c.TLSKey,
			serverName: c.TLSServerName,
			insecure:   c.TLSInsecure,
			enabled:    c.UseTLS,
		}, out.Broker)
		if err != nil {
			return nil, err
		}
		out.TLS = cfg
	}
	return out, nil
}

// tlsInline carries the inline TLS fields merged from the two config
// layers (config node wins, inline fills gaps) for resolveTLS.
type tlsInline struct {
	ca, cert, key, serverName string
	insecure, enabled         bool
}

// resolveTLS materializes the *tls.Config for a connection: a tlsConfig
// node reference first, then inline PEM paths, then a bare default when
// usetls or an ssl://-style URL already implies TLS. InsecureSkipVerify
// is applied on top of whatever the material resolved to.
func resolveTLS(host NodeHost, workflowID, tlsRef string, in tlsInline, broker string) (*tls.Config, error) {
	var cfg *tls.Config
	if tlsRef != "" {
		if host == nil {
			return nil, fmt.Errorf("tls config %q referenced but no host lookup is available", tlsRef)
		}
		resolved, err := host.LoadTLSConfig(workflowID, tlsRef)
		if err != nil {
			return nil, err
		}
		cfg = resolved
	} else if in.ca != "" || in.cert != "" || in.key != "" {
		tc := &TLSConfig{
			CertPath:         in.cert,
			KeyPath:          in.key,
			CAPath:           in.ca,
			ServerName:       in.serverName,
			VerifyServerCert: !in.insecure,
		}
		if (tc.CertPath == "") != (tc.KeyPath == "") {
			return nil, errors.New("mqtt tls: cert and key must be configured together")
		}
		built, err := tc.Build()
		if err != nil {
			return nil, err
		}
		cfg = built
	}

	needsTLS := cfg != nil || in.enabled || strings.HasPrefix(broker, "ssl://") ||
		strings.HasPrefix(broker, "tls://") || strings.HasPrefix(broker, "wss://")
	if !needsTLS {
		return nil, nil
	}
	if cfg == nil {
		cfg = &tls.Config{}
	}
	if in.insecure {
		cfg.InsecureSkipVerify = true //nolint:gosec // explicit user opt-out, Node-RED parity
	}
	return cfg, nil
}

// brokerAddress combines a Node-RED style host + separate port into a
// single address. A Broker carrying a scheme or its own :port wins.
func brokerAddress(host, port string) string {
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	if host == "" || port == "" {
		return host
	}
	if strings.Contains(host, "://") || strings.Contains(host, ":") {
		return host
	}
	return host + ":" + port
}

// MQTTClientOptions builds the paho client options for resolved dial
// options — exported so dial-option construction is testable without a
// broker.
func MQTTClientOptions(o *MQTTOpts, clientID string) *mqtt.ClientOptions {
	if o.ClientID != "" {
		clientID = o.ClientID
	}
	addr := strings.TrimSpace(o.Broker)
	scheme := ""
	if i := strings.Index(addr, "://"); i >= 0 {
		scheme = addr[:i]
	}
	// paho picks the transport off the URL scheme alone — a tcp:// (or
	// schemeless) address dials PLAINTEXT even when a tls.Config is
	// set. TLS material must therefore rewrite the scheme, or usetls
	// silently speaks cleartext to the broker.
	if o.TLS != nil {
		switch scheme {
		case "", "tcp", "mqtt":
			scheme = "ssl"
		case "ws":
			scheme = "wss"
		}
	}
	if scheme == "" {
		scheme = "tcp"
	}
	if i := strings.Index(addr, "://"); i >= 0 {
		addr = scheme + addr[i:]
	} else {
		addr = scheme + "://" + addr
	}
	opts := mqtt.NewClientOptions().
		AddBroker(addr).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetConnectTimeout(10 * time.Second).
		SetCleanSession(o.CleanSession)
	if o.KeepAlive > 0 {
		opts.SetKeepAlive(time.Duration(o.KeepAlive) * time.Second)
	}
	if o.Username != "" {
		opts.SetUsername(o.Username).SetPassword(o.Password)
	}
	if o.TLS != nil {
		opts.SetTLSConfig(o.TLS)
	}
	if o.WillTopic != "" {
		opts.SetWill(o.WillTopic, o.WillPayload, byte(ClampQoS(o.WillQoS)), o.WillRetain)
	}
	return opts
}

// MQTTConnectOpts dials a broker with the resolved options. clientID is
// the caller's default (per node id); o.ClientID wins when configured.
func MQTTConnectOpts(o *MQTTOpts, clientID string) (mqtt.Client, error) {
	if strings.TrimSpace(o.Broker) == "" {
		return nil, errors.New("mqtt broker address is empty")
	}
	client := mqtt.NewClient(MQTTClientOptions(o, clientID))
	token := client.Connect()
	if !token.WaitTimeout(12 * time.Second) {
		return nil, fmt.Errorf("mqtt connect to %s timed out", o.Broker)
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt connect to %s failed: %w", o.Broker, err)
	}
	return client, nil
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

func firstNonZero(v ...int) int {
	for _, n := range v {
		if n != 0 {
			return n
		}
	}
	return 0
}
