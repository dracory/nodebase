# Tail node

Type: `tail`  
Category: `Storage`

Follows a file and emits newly appended lines.

## Behavior

This node is registered as `tail` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `filename` | `` |  |  |
| `filetype` | `string` | `text` | File Type — text: one message per appended line; binary: one []byte message per appended chunk |
| `fromStart` | `boolean` | `false` | Read From Start — Emit the existing file content first instead of starting at the end |
| `interval` | `number` | `1` | Poll Interval (seconds) — Polling period for new content; 0 = 1 second |
| `split` | `string` | `\n` | Separator — Record separator for text mode (default \n; escapes allowed) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
