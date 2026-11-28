# CSV node

Type: `csv`  
Category: `Parsers`

Parses CSV into one message per row, or serializes rows to CSV.

## Behavior

This node is registered as `csv` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `headers` | `boolean` | `false` | First Row is Header — Parse: rows become objects keyed by the header line; serialize: write a header row from object keys |
| `property` | `string` | `payload` | Property — Field to convert; "payload" (default) or a dotted path inside the payload |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
