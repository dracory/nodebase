# RBE node

Type: `rbe`  
Category: `Flow Control`

Only forwards the message when the watched property changed.

## Behavior

This node is registered as `rbe` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `func` | `string` | `rbe` | Mode — rbe: block until the value changes; rbei: pass the first message; narrowband/deadband: compare against a gap around the last passed value |
| `gap` | `string` | `0` | Gap — deadband/narrowband threshold; a trailing % makes it a percentage of the last value |
| `initialValue` | `string` |  | Initial Value — Seed the previous value before any message arrives (JSON or plain string) |
| `property` | `string` | `payload` | Property — Field to watch; "payload" (default) or a dotted path inside the payload |
| `septopics` | `boolean` | `false` | Separate Topics — Keep a separate previous value per message topic |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
