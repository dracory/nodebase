# AI Provider node

Type: `ai-provider`  
Category: `AI`

Shared LLM provider definition referenced by ai-request nodes.

## Behavior

This node is registered as `ai-provider` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `apiKey` | `string` |  | API Key — API key (credential: masked, never exported); optional for endpoints that need no auth |
| `baseURL` | `string` |  | Base URL — Endpoint URL — required for provider "custom" (e.g. http://localhost:11434/v1/chat/completions) |
| `model` | `string` |  | Default Model — Model used by referencing nodes unless the provider supplies a default |
| `provider` | `string` | `custom` | Provider — LLM provider; "custom" targets any OpenAI-compatible endpoint (Ollama, llama.cpp, vLLM) |
| `providerOptions` | `object` |  | Provider Options — Provider-specific options merged into the request (escape hatch) |
| `timeoutSeconds` | `number` |  | Timeout (s) — Default per-call timeout in seconds; the referencing node's timeout wins when set |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
