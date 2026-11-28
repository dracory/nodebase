# WS Out node

Type: `websocketOut`  
Category: `Network`

Writes the message to a websocket session or broadcasts to all.

## Behavior

This node is registered as `websocketOut` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `target` | `string` | `session` | Target — session: write back on the connection named by the message's sessionId metadata; broadcast: write to every open session |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
