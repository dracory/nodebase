# WS Listener node

Type: `websocketListener`  
Category: `Network`

Shared websocket listener endpoint referenced by websocketIn/websocketOut nodes.

## Behavior

This node is registered as `websocketListener` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | `string` | `/ws` | Path — Endpoint path served on the host's shared HTTP listener |
| `wholemsg` | `string` | `false` | Whole Message |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
