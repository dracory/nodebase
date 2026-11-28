# Condition node

Type: `condition`  
Category: `Flow Control`

Conditionally executes different branches of the workflow.

## Behavior

This node is registered as `condition` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `op` | `string` | `eq` | Operator |
| `property` | `` |  |  |
| `value` | `` |  |  |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
