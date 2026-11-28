# Wait node

Type: `wait`  
Category: `Flow Control`

Durably parks messages until a distributed notify signal reaches its target count.

## Behavior

This node is registered as `wait` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `mergeMode` | `string` | `wait-wins` | Notification Metadata Merge — wait-wins keeps waiting-message metadata on key conflicts; notify-wins lets cached notification metadata overwrite it |
| `pollIntervalMs` | `number` | `500` | Poll Interval Milliseconds — How often the deployed wait node checks the durable coordination store; lower values react faster but cost more queries |
| `signal` | `string` |  | Release Signal — Fixed text or a mustache template such as {{payload.id}}; the evaluated value is trimmed and becomes the global distributed signal key; keys are case-sensitive and shared across workflows |
| `targetCount` | `string` | `1` | Target Signal Count — Fixed positive integer or a mustache template that evaluates to one; exactly this many signal increments are consumed on release |
| `timeoutSeconds` | `number` | `0` | Wait Timeout Seconds — 0 waits indefinitely; positive values route the parked message to output_2 when the wait expires |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
