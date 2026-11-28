# AI Approval node

Type: `ai-approval`  
Category: `AI`

Pauses the flow until a human reply keyed by approvalId arrives; request leaves on output 2.

## Behavior

This node is registered as `ai-approval` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `question` | `string` |  | Question — Mustache template rendered against the message — the question the approver answers; empty uses the payload as-is |
| `timeoutSeconds` | `number` |  | Timeout (s) — How long to await the reply; 0 waits until the workflow stops |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
