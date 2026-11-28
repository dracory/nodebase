# WS In node

Type: `websocketIn`  
Category: `Network`

Accepts websocket connections on its own listener and emits received messages.

## Behavior

This node is registered as `websocketIn` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `allowedOrigins` | `string` | `` | Allowed Origins — Comma-separated Origin host patterns allowed to connect (e.g. "example.com,*.example.com"). Empty accepts any origin — cross-site websocket hijacking exposure; set this on browser-facing flows |
| `path` | `string` | `/ws` | Path — Endpoint path that accepts websocket upgrades |
| `port` | `integer` | `8082` | Port — The node's own listen port |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
