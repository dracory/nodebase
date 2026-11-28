# Switch node

Type: `switch`  
Category: `Flow Control`

Routes the message to one of several outputs based on rules.

## Behavior

This node is registered as `switch` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `mode` | `string` | `first` | Mode — first: route to the first matching output only; all: route to every matching output |
| `otherwise` | `boolean` | `false` | Otherwise Port — Adds a port after the last rule output that receives messages matching no rule |
| `rules` | `array` |  | Rules — Ordered list of rules, each {property, op, value}. Property is a dotted path into the payload; rule i routes to output_{i+1}. |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
