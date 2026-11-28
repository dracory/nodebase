# Doc To Text node

Type: `docToText`  
Category: `Parsers`

Extracts text from a document payload (pdf/docx/odt/pptx/txt).

## Behavior

This node is registered as `docToText` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `format` | `string` | `auto` | Format — auto detects the format from the payload's magic bytes |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
