# UDP Out node

Type: `udpOut`  
Category: `Network`

Sends a UDP datagram.

## Behavior

This node is registered as `udpOut` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `addr` | `string` |  | Address — Target address (default 127.0.0.1; broadcast address for broad, group for multi); the message's ip metadata overrides it |
| `base64` | `boolean` | `false` | Decode Base64 — Decode a base64 payload string before sending |
| `host` | `` |  |  |
| `ipv` | `string` | `udp4` | Using |
| `multicast` | `string` | `false` | Send — false: a UDP datagram; broad: a broadcast message; multi: a multicast message to the group address |
| `outport` | `integer` |  | Local Port — Bind the local port instead of using a random one |
| `port` | `integer` |  | Port — Target port; the message's port metadata overrides it |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
