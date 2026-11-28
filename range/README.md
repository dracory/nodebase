# Range node

Type: `range`  
Category: `Flow Control`

Maps a numeric property from an input range to an output range.

## Behavior

This node is registered as `range` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `action` | `string` | `scale` | Action — scale: extrapolate freely; drop: discard out-of-range messages; clamp: clamp to the input range; roll: wrap into the input range |
| `maxin` | `number` | `100` | Input Max — Upper bound of the input range |
| `maxout` | `number` | `1` | Output Max — Upper bound of the output range |
| `minin` | `number` | `0` | Input Min — Lower bound of the input range |
| `minout` | `number` | `0` | Output Min — Lower bound of the output range |
| `property` | `string` | `payload` | Property — Field to map; "payload" (default) or a dotted path inside the payload |
| `round` | `boolean` | `false` | Round Result — Round the scaled value to the nearest integer |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
