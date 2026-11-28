# File In node

Type: `fileIn`  
Category: `Storage`

Reads a file into the payload, whole or one message per line.

## Behavior

This node is registered as `fileIn` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `errorCatch` | `boolean` | `false` | Error Catch Port — Adds a dedicated "error" output port; a node failure emits msg.error={text,node,full} on it when wired, else the catch/problem-queue ladder applies |
| `filename` | `` |  |  |
| `filenameType` | `string` | `fixed` | Filename Source |
| `format` | `string` | `utf8` | Format — utf8: one string message; lines: one message per line (parts metadata); stream: one message per 64KiB chunk; binary: one []byte message |
| `sendError` | `boolean` | `false` | Send Error — Emit an error message for a missing file instead of raising a catchable error |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
