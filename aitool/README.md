# AI Tool node

Type: `ai-tool`  
Category: `AI`

Entry point of a flow exposed to an ai-agent as a callable tool.

## Behavior

This node is registered as `ai-tool` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `description` | `string` |  | Description — Tells the model when to use this tool |
| `parameters` | `object` |  | Parameters — JSON Schema object describing the tool arguments; empty sends an open object schema |
| `toolName` | `string` |  | Tool Name — Function name the model calls (letters, digits, _ and -); defaults to the node id |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
