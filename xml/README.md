# XML node

Type: `xml`  
Category: `Parsers`

Converts between an XML string and a generic map.

## Behavior

This node is registered as `xml` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `attr` | `string` | `$` | Attribute Key — Object key element attributes are collected under (default "$"); an empty value merges attributes into the element object |
| `chr` | `string` | `_` | Character Key — Object key element text content is stored under (default "_") |
| `pretty` | `boolean` | `false` | Pretty Print — Indent marshalled output with two spaces |
| `property` | `string` | `payload` | Property — Field to convert; "payload" (default) or a dotted path inside the payload |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
