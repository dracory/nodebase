# Split node

Type: `split`  
Category: `Flow Control`

Splits a string, array or object into a sequence of messages.

## Behavior

This node is registered as `split` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `addname` | `string` |  | Add Name — Optional field name on each output message receiving the part's key (objects) or index |
| `arraySplt` | `integer` | `1` | Array Chunk Length — Number of array elements per emitted message |
| `property` | `string` | `payload` | Property — Field to split; "payload" (default) or a dotted path inside the payload |
| `splt` | `string` | `\n` | Split Using — Delimiter for strings (escape sequences like \n allowed) or chunk length when spltType is "len" |
| `spltType` | `string` | `str` | Split Type |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
