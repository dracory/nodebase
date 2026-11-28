# HTTP Request node

Type: `httpRequest`  
Category: `Actions`

Makes an HTTP request to a specified URL.

## Behavior

This node is registered as `httpRequest` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `body` | `` |  |  |
| `errorCatch` | `boolean` | `false` | Error Catch Port — Adds a dedicated "error" output port; a node failure emits msg.error={text,node,full} on it when wired, else the catch/problem-queue ladder applies |
| `headers` | `object` |  | Headers |
| `method` | `string` | `GET` | HTTP Method |
| `password` | `string` |  | Password — Optional basic-auth password (credential: masked, never exported) |
| `url` | `` |  |  |
| `username` | `string` |  | Username — Optional basic-auth username (credential: masked, never exported) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
