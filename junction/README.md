# Junction node

Type: `junction`  
Category: `Flow Control`

A pass-through bend point for organising wires.

## Behavior

This node is registered as `junction` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

This node has no schema-declared configuration fields beyond the common node name and placement metadata.

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
