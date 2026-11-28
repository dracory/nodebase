# MQTT Broker node

Type: `mqttBroker`  
Category: `Network`

Shared MQTT broker connection referenced by mqttIn/mqttOut nodes.

## Behavior

This node is registered as `mqttBroker` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `broker` | `` |  |  |
| `cleansession` | `boolean` | `true` | Clean Session |
| `clientid` | `` |  |  |
| `keepalive` | `integer` | `0` | Keep Alive — MQTT keepalive in seconds; 0 uses the library default |
| `password` | `string` |  | Password — Optional broker password (credential: masked, never exported) |
| `port` | `` |  |  |
| `tls` | `` |  |  |
| `tlsca` | `` |  |  |
| `tlscert` | `` |  |  |
| `tlskey` | `` |  |  |
| `tlsservername` | `` |  |  |
| `user` | `string` |  | Username — Optional broker username (credential: masked, never exported) |
| `usetls` | `boolean` | `false` | Use TLS — Dial the broker over TLS (implied by an ssl:// address) |
| `verifyservercert` | `boolean` | `true` | Verify Peer Certificate — false disables peer verification (insecure; Node-RED verifyservercert) |
| `willpayload` | `` |  |  |
| `willqos` | `integer` | `0` | Will QoS |
| `willretain` | `boolean` | `false` | Will Retain |
| `willtopic` | `` |  |  |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
