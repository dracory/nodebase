# Watch node

Type: `watch`  
Category: `Storage`

Emits a message for each file change under a watched path.

## Behavior

This node is registered as `watch` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `filename` | `` |  |  |
| `interval` | `number` | `1` | Poll Interval (seconds) — Polling period for change detection; 0 = 1 second |
| `recursive` | `boolean` | `false` | Recursive — Watch subdirectories when Path is a directory |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
