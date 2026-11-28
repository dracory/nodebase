# PDF node

Type: `pdf`  
Category: `Document`

Renders the payload to a PDF document (invoices, reports).

## Behavior

This node is registered as `pdf` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `filename` | `` |  |  |
| `mode` | `string` | `text` | Mode — text: payload string becomes a paginated text PDF; template: payload {title, fields, table, body} drives a report layout |
| `title` | `` |  |  |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
