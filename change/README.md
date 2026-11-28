# Change node

Type: `change`  
Category: `Flow Control`

Sets, moves or deletes fields on the payload.

## Behavior

This node is registered as `change` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `rules` | `array` |  | Rules — Ordered list of rules, each {op: set\|delete\|move, field, value}. Field is a dotted path; "payload" targets the whole payload. |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
