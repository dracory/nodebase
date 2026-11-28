# AI Memory node

Type: `ai-memory`  
Category: `AI`

Shared conversation memory referenced by ai-agent nodes.

## Behavior

This node is registered as `ai-memory` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `keyProperty` | `string` | `msg.conversationId` | Conversation Key — Message property holding the conversation id ("msg.x" metadata or a payload path); empty/unset falls back to "default" |
| `maxMessages` | `integer` | `40` | Context Window — Maximum number of stored turns loaded into the prompt — the oldest are dropped first |
| `retentionSeconds` | `number` |  | Retention (s) — Forget turns older than this many seconds; 0 keeps them until the window cap evicts them |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
