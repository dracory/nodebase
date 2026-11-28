# MQTT Out node

Type: `mqttOut`  
Category: `Network`

Publishes the message to an MQTT broker topic.

## Behavior

This node is registered as `mqttOut` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `broker` | `` |  |  |
| `cleansession` | `boolean` | `true` | Clean Session |
| `clientid` | `` |  |  |
| `keepalive` | `integer` | `0` | Keep Alive — MQTT keepalive in seconds; 0 uses the library default |
| `password` | `string` |  | Password — Optional broker password (credential: masked, never exported) |
| `qos` | `integer` | `0` | QoS — Publish QoS; when blank the message's qos metadata is used instead |
| `retain` | `boolean` | `false` | Retain — Retain flag; when blank the message's retain metadata is used instead |
| `tls` | `` |  |  |
| `tlsca` | `` |  |  |
| `tlscert` | `` |  |  |
| `tlsinsecure` | `boolean` | `false` | Skip Peer Verification — Disables broker certificate verification (insecure opt-out) |
| `tlskey` | `` |  |  |
| `tlsservername` | `` |  |  |
| `topic` | `` |  |  |
| `username` | `string` |  | Username — Optional broker username (credential: masked, never exported) |
| `usetls` | `boolean` | `false` | Use TLS — Dial the broker over TLS (implied by an ssl:// address) |
| `willpayload` | `` |  |  |
| `willqos` | `integer` | `0` | Will QoS |
| `willretain` | `boolean` | `false` | Will Retain |
| `willtopic` | `` |  |  |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
