# AI Request node

Type: `ai-request`  
Category: `AI`

Prompts an LLM via an ai-provider config and writes the parsed reply to a msg property.

## Behavior

This node is registered as `ai-request` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `concurrency` | `integer` |  | Concurrency — Maximum in-flight LLM calls; additional messages wait. Empty/0 means unlimited |
| `maxTokens` | `integer` |  | Max Tokens — Response token cap; empty uses the provider default |
| `parseMode` | `string` | `text` | Parse — How the response text is interpreted before it is written to the target |
| `prompt` | `string` |  | User Prompt — Mustache template rendered against the message ({{payload.x}}, {{env.X}}, {{flow.X}}, {{global.X}}) |
| `provider` | `string` |  | AI Provider — ID of the ai-provider config node to call through |
| `systemPrompt` | `string` |  | System Prompt — Optional system prompt sent verbatim |
| `target` | `string` | `payload` | Target — Message property the result is written to: "payload" or a (dotted) msg property such as msg.llm.answer |
| `temperature` | `number` |  | Temperature — Sampling temperature; empty uses the provider default |
| `timeoutSeconds` | `number` |  | Timeout (s) — Per-call timeout in seconds; overrides the provider default when set |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
