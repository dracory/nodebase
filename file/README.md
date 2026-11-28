# File node

Type: `file`  
Category: `Storage`

Writes the payload to a file (optionally restricted to a root directory).

## Behavior

This node is registered as `file` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `appendNewline` | `boolean` | `false` | Append Newline — Append \n to every written payload |
| `createDir` | `boolean` | `false` | Create Directory — Create missing parent directories (mkdir -p) |
| `encoding` | `string` | `utf8` | Encoding — base64/hex decode a string payload before writing |
| `errorCatch` | `boolean` | `false` | Error Catch Port — Adds a dedicated "error" output port; a node failure emits msg.error={text,node,full} on it when wired, else the catch/problem-queue ladder applies |
| `filename` | `` |  |  |
| `filenameType` | `string` | `fixed` | Filename Source |
| `overwriteFile` | `string` | `true` | If File Exists — true: overwrite (truncate); false: append to the file; delete: remove the file first, then write fresh |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
