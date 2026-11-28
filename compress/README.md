# Compress node

Type: `compress`  
Category: `Transform`

Compresses or decompresses a whole message property with gzip, zlib, or raw flate.

## Behavior

This node is registered as `compress` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `algorithm` | `string` | `gzip` | Algorithm — flate and deflate are aliases for raw DEFLATE data |
| `operation` | `string` | `compress` | Operation |
| `property` | `string` | `payload` | Property — Message property to process as one whole byte sequence; payload, dotted payload-map paths, or msg.* metadata paths |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
