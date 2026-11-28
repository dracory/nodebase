# Link Out node

Type: `linkOut`  
Category: `Flow Control`

Pushes the message to Link In nodes, or returns it to a Link Call.

## Behavior

This node is registered as `linkOut` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `links` | `array` |  | Links — linkIn nodes that receive pushed messages (same workflow) |
| `mode` | `string` | `link` | Mode — link: push the message to every linked linkIn; return: answer the calling linkCall |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
