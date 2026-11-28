# Status node

Type: `status`  
Category: `Flow Control`

Receives status-change events from nodes in scope.

## Behavior

This node is registered as `status` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `nodes` | `string` |  | Nodes — Comma-separated node ids to watch (only for scope "selected") |
| `scope` | `string` | `all` | Scope — all: receive status changes from every node; selected: only from the listed nodes |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
