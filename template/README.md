# Template node

Type: `template`  
Category: `Flow Control`

Renders a template against the payload.

## Behavior

This node is registered as `template` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `field` | `string` | `payload` | Property — Message property (dotted path) or context key the result is written to |
| `fieldType` | `string` | `msg` | Set |
| `output` | `string` | `str` | Output Format |
| `syntax` | `string` | `mustache` | Syntax — mustache = Node-RED parity; gotemplate = Go text/template (extension-only); plain = no rendering |
| `template` | `string` |  | Template — Template rendered against the message (e.g. {"echo": "{{payload.body.name}}"}) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
