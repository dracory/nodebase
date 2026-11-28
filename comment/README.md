# Comment node

Type: `comment`  
Category: `Common`

Canvas annotation text; never executes.

## Behavior

This node is registered as `comment` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `info` | `string` |  | Comment — Annotation text rendered on the canvas; the node never executes |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
