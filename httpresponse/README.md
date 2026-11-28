# HTTP Response node

Type: `httpResponse`  
Category: `Actions`

Answers the request that triggered the flow.

## Behavior

This node is registered as `httpResponse` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `headers` | `object` |  | Headers — Extra response headers; a headers object in the message merges on top |
| `statusCode` | `integer` | `200` | Status Code — HTTP status code (per-message statusCode/headers in the payload take precedence) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
