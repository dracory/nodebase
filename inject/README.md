# Inject node

Type: `inject`  
Category: `Triggers`

Manually or periodically injects a message.

## Behavior

This node is registered as `inject` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `interval` | `number` | `0` | Interval — Repeat period in Interval Units; 0 = no repeat |
| `intervalUnits` | `string` | `s` | Interval Units — Time unit for the repeat interval |
| `once` | `boolean` | `false` | Fire Once on Deploy — Inject the payload immediately when the workflow is deployed |
| `onceDelay` | `number` | `0.1` | After a Delay of — Seconds to wait before the initial inject (Node-RED's onceDelay, default 0.1) |
| `payload` | `string` |  | Payload — Data injected as the message — interpreted per Payload Type (unused for timestamp/date) |
| `payloadType` | `string` | `timestamp` | Payload Type — timestamp/date: current time in ms · json: parse the payload field · string: literal text · num: parsed number · bool: true/false · env: payload names an environment variable |
| `props` | `array` |  | Properties — Node-RED-style message properties to inject — each {p, v, vt}. "payload" sets msg.Payload, "topic" the topic, other names land in message metadata. |
| `repeatType` | `string` | `none` | Repeat |
| `times` | `array` |  | Times — Times of day to fire (Node-RED's "at a specific time") — up to three entries |
| `topic` | `string` |  | Topic — Optional topic stamped into the message metadata (Node-RED msg.topic) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
