# Join node

Type: `join`  
Category: `Flow Control`

Reassembles a message sequence into a single message.

## Behavior

This node is registered as `join` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `build` | `string` | `array` | Build |
| `count` | `integer` |  | Count — Custom mode: number of messages per group |
| `joiner` | `string` |  | Joiner — Delimiter for the "string" build (escape sequences like \n allowed) |
| `key` | `string` |  | Group Key — Custom mode object build: message property whose value keys the object (default "topic") |
| `mode` | `string` | `auto` | Mode — auto: reassemble a split sequence via parts metadata; custom: join by count/timeout/complete; reduce: fold the sequence with an expression |
| `property` | `string` | `payload` | Property — Message property to join; "payload" (default) or a message path |
| `timeout` | `number` | `0` | Timeout (seconds) — Custom mode: flush an incomplete group after this delay; 0 = wait forever |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
