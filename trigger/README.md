# Trigger node

Type: `trigger`  
Category: `Flow Control`

Sends one message immediately, then optionally a second after a delay.

## Behavior

This node is registered as `trigger` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bytopic` | `boolean` | `false` | Separate Timers per Topic — Keep an independent timer per message topic |
| `duration` | `number` | `250` | Delay — Delay before the second output fires; negative = resend every interval, 0 = wait to be reset |
| `extend` | `boolean` | `false` | Extend on New Message — Restart the pending timer when a new message arrives |
| `op1` | `string` |  | First Output — Value sent immediately on output 1 (parsed per op1type) |
| `op1type` | `string` | `str` | First Output Type |
| `op2` | `string` |  | Second Output — Value sent after the delay (parsed per op2type) |
| `op2type` | `string` | `str` | Second Output Type |
| `outputs` | `integer` | `2` | Outputs — 2 sends the delayed output on a second port; 1 reuses output 1 |
| `overrideDelay` | `boolean` | `false` | Override via msg.delay — Let the message's delay metadata replace the configured delay (same units) |
| `reset` | `string` |  | Reset Payload — A message whose payload equals this value resets the trigger (msg.reset always does) |
| `resetValue` | `string` |  | Reset Value — Alias for reset — a message whose payload equals it resets the trigger |
| `then` | `string` | `wait` | Then — wait: send the second output after the delay; loop: resend the first output every interval until reset; block: wait to be reset (imported flows also derive the mode from the delay sign — negative = resend, zero = wait to be reset) |
| `units` | `string` | `ms` | Units |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
