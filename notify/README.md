# Notify node

Type: `notify`  
Category: `Flow Control`

Atomically increments a durable distributed release signal.

## Behavior

This node is registered as `notify` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `cacheKeys` | `string` |  | Metadata Keys To Cache — Comma-separated message metadata keys copied into the signal cache for released wait messages; nested metadata paths use dots |
| `signal` | `string` |  | Release Signal — Fixed text or a mustache template such as {{payload.id}}; the evaluated value is trimmed and becomes the global distributed signal key; keys are case-sensitive and shared across workflows |
| `signalTTLSeconds` | `number` | `0` | Signal TTL Seconds — 0 keeps unconsumed signal counts indefinitely; positive values expire the whole unconsumed count after the latest increment |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
