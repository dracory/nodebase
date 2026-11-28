# Complete node

Type: `complete`  
Category: `Flow Control`

Fires when a node in scope finishes handling a message.

## Behavior

This node is registered as `complete` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `nodes` | `string` |  | Nodes — Comma-separated node ids to watch (only for scope "selected") |
| `scope` | `string` | `all` | Scope — all: fire when any node's message finishes; selected: only for the listed nodes |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
