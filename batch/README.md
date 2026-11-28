# Batch node

Type: `batch`  
Category: `Flow Control`

Groups incoming messages into batch sequences.

## Behavior

This node is registered as `batch` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `allowEmptySequence` | `boolean` | `false` | Allow Empty Sequence — interval mode: emit an empty sequence message on a window with no input |
| `count` | `integer` | `10` | Count — count mode: number of messages per batch sequence |
| `honourParts` | `boolean` | `false` | Honour Parts — count mode: flush when an incoming parts sequence completes |
| `interval` | `number` | `10` | Interval (seconds) — interval mode: window length |
| `mode` | `string` | `count` | Mode |
| `overlap` | `integer` | `0` | Overlap — count mode: messages kept for the next batch |
| `topics` | `array` |  | Topics — concat mode: topics whose sequences are concatenated, in output order |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
