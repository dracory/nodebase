# AI Agent node

Type: `ai-agent`  
Category: `AI`

Runs the model ↔ tool-call loop: output 1 = final answer, output 2 = tool-call trace.

## Behavior

This node is registered as `ai-agent` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `concurrency` | `integer` |  | Concurrency — Maximum in-flight agent runs; additional messages wait. Empty/0 means unlimited |
| `maxIterations` | `integer` | `8` | Max Iterations — Mandatory cap on model ↔ tool rounds; exceeding it routes an error to the catch path |
| `memory` | `string` |  | AI Memory — Optional ID of an ai-memory config node supplying conversation history |
| `prompt` | `string` |  | User Prompt — Mustache template rendered against the message ({{payload.x}}, {{env.X}}, {{flow.X}}, {{global.X}}) |
| `provider` | `string` |  | AI Provider — ID of the ai-provider config node to call through |
| `systemPrompt` | `string` |  | System Prompt — Optional system prompt sent verbatim |
| `target` | `string` | `payload` | Target — Message property the final answer is written to |
| `timeoutSeconds` | `number` |  | Model Timeout (s) — Per-call timeout for each model round; overrides the provider default when set |
| `toolTimeoutSeconds` | `number` | `60` | Tool Timeout (s) — How long each tool call may run before its result is reported back as an error |
| `tools` | `string` |  | Tools — Comma-separated ai-tool references: "nodeID" (same workflow) or "workflowID/nodeID" |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
