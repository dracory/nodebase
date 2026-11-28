# WS Client node

Type: `websocketClient`  
Category: `Network`

Shared outbound websocket connection referenced by websocketIn/websocketOut nodes.

## Behavior

This node is registered as `websocketClient` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | `string` |  | URL — Full ws:// or wss:// URL to connect to |
| `tls` | `string` |  | TLS Config — Optional tlsConfig node id for wss connections |
| `wholemsg` | `string` | `false` | Whole Message |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
