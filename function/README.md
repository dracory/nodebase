# Function node

Type: `function`  
Category: `Flow Control`

Runs arbitrary JavaScript over the message (goja sandbox).

## Behavior

This node is registered as `function` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `finalize` | `string` |  | On Stop (JavaScript) — Node-RED close code — runs when the node stops |
| `func` | `string` | `return msg;` | Function (JavaScript) — JavaScript body over msg — return msg to pass it on, node.send([m1,m2]) for multi-output, node.error(str) to raise |
| `initialize` | `string` |  | On Start (JavaScript) — Node-RED setup code — runs once when the node activates; a returned promise is awaited before messages flow |
| `outputs` | `integer` | `1` | Outputs — Declared output ports — the editor draws one static output; extra ports need dynamic-port support (future work) |
| `timeout` | `number` | `0` | Timeout (ms) — Maximum script runtime before the VM is interrupted (0 = no timeout, Node-RED default) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
