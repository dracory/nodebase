# HTML node

Type: `html`  
Category: `Parsers`

Extracts elements/text/attributes from HTML via a CSS selector.

## Behavior

This node is registered as `html` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `as` | `string` | `single` | Output — single: one message with an array payload; multi: one parts-tagged message per match |
| `outproperty` | `string` | `payload` | Output Property — Message property the extraction is written to (defaults to property) |
| `property` | `string` | `payload` | Property — Message property holding the HTML |
| `ret` | `string` | `html` | Return — html: element outer HTML; text: element text; val: the value attribute; any other value names an attribute (Node-RED semantics) |
| `tag` | `string` |  | Selector — CSS selector. Supported subset: tag, .class, #id, [attr], [attr=value], descendant and > combinators, comma groups |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
