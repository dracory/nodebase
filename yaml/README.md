# YAML node

Type: `yaml`  
Category: `Parsers`

Converts between a YAML string and its object form.

## Behavior

This node is registered as `yaml` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `action` | `string` | `` | Action — auto (empty): stringify objects, parse strings; str: always convert to a YAML string; obj: always parse a YAML string |
| `outputs` | `integer` | `1` | Outputs — 2 adds a second port that receives messages whose YAML failed to parse |
| `property` | `string` | `payload` | Property — Field to convert; "payload" (default) or a dotted path inside the payload |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
