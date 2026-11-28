# Unknown node

Type: `unknown`  
Category: `Common`

Placeholder for unrecognized node types; excluded from execution.

## Behavior

This node is registered as `unknown` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

This node has no schema-declared configuration fields beyond the common node name and placement metadata.

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
