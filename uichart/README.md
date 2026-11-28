# UI Chart node

Type: `uiChart`  
Category: `Dashboard`

Dashboard live chart: numeric or array payload, line or bar.

## Behavior

This node is registered as `uiChart` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `group` | `string` |  | Group — Optional section inside the page |
| `kind` | `string` | `line` | Kind |
| `label` | `string` |  | Label — Widget caption; falls back to the node name then id |
| `order` | `number` |  | Order — Optional sort hint within the group |
| `page` | `string` | `default` | Page — Dashboard page the widget renders on |
| `points` | `number` | `50` | Points — Rolling series length kept on the client |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
