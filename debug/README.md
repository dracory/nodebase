# Debug node

Type: `debug`  
Category: `Debug`

Displays the message payload in the debug panel.

## Behavior

This node is registered as `debug` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `active` | `boolean` | `true` | Active — false mutes the node — deployed but emits nothing |
| `complete` | `string` | `payload` | Output — payload (or empty, and Node-RED's legacy "false"): the message data · true: complete msg object incl. metadata · any other string is a msg.<path> (e.g. "topic" or "payload.user") · with targetType "jsonata" it is a JSONata expression evaluated against the message. |
| `console` | `boolean` | `false` | Also Print to Console — Print the formatted value to stdout as well |
| `statusVal` | `string` |  | Status Value — Optional property path ("payload" or a dotted path) also shown as node status text |
| `tosidebar` | `boolean` | `true` | Show in Debug Panel — false suppresses the debug-panel event (Node-RED tosidebar) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
