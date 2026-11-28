# Delay node

Type: `delay`  
Category: `Flow Control`

Delays each message or limits the rate at which they pass (Node-RED delay).

## Behavior

This node is registered as `delay` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `allowrate` | `boolean` | `false` | Allow msg.rate Override — A numeric msg.rate (in ms) replaces the interval until reset |
| `delayPath` | `string` | `delay` | Delay Property — Message property holding the delay in milliseconds (e.g. "delay" for msg.delay) |
| `drop` | `boolean` | `false` | Drop Intermediate Messages — Keep only the most recent message while rate-limited (off queues them all) |
| `nbRateUnits` | `number` | `1` | Per |
| `outputs` | `integer` | `1` | Outputs — 2 adds a second port that receives messages dropped by the rate limiter |
| `pauseType` | `string` | `delay` | Action |
| `randomFirst` | `number` | `1` | Between |
| `randomLast` | `number` | `5` | And |
| `randomUnits` | `string` | `seconds` | Units |
| `rate` | `integer` | `1` | Rate — Messages allowed per rate period — they are spread evenly across it |
| `rateType` | `string` | `all` | Apply To |
| `rateUnits` | `string` | `second` | Rate Units |
| `timeout` | `number` | `5` | For — Fixed delay before each message is released |
| `timeoutUnits` | `string` | `seconds` | Units |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
