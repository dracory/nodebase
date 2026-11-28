# UI Notification node

Type: `uiNotification`  
Category: `Dashboard`

Dashboard toast from an incoming message (text/level).

## Behavior

This node is registered as `uiNotification` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `group` | `string` |  | Group — Optional section inside the page |
| `label` | `string` |  | Label — Widget caption; falls back to the node name then id |
| `order` | `number` |  | Order — Optional sort hint within the group |
| `page` | `string` | `default` | Page — Dashboard page the widget renders on |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
