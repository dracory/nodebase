# Sort node

Type: `sort`  
Category: `Flow Control`

Sorts a payload array or a message sequence.

## Behavior

This node is registered as `sort` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `as_num` | `boolean` | `false` | Numeric Compare — Coerce compared values to numbers first |
| `msgKey` | `string` |  | Element Key — msg target: per-element key — evaluated as a sequence expression when msgKeyType is "jsonata" |
| `order` | `string` | `ascending` | Order |
| `seqKey` | `string` |  | Sequence Key — seq target: message path or sequence expression used as sort key (default "payload") |
| `target` | `string` |  | Target Property — msg target: message property holding the array to sort (default "payload") |
| `targetType` | `string` | `msg` | Target — msg: sort the array at target in place; seq: sort a message sequence by seqKey |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
