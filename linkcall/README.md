# Link Call node

Type: `linkCall`  
Category: `Flow Control`

Calls a Link In component and awaits its Link Out return.

## Behavior

This node is registered as `linkCall` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `link` | `string` |  | Link — Target linkIn node (static mode) |
| `linkType` | `string` | `static` | Link Type — static: use the configured link; dynamic: take the target from message metadata "target" |
| `timeout` | `number` | `30` | Timeout (seconds) — How long to await the linkOut return; 0 = 30 seconds |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
