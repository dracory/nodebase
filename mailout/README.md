# Mail Out node

Type: `mailOut`  
Category: `Network`

Sends the message as an e-mail via SMTP.

## Behavior

This node is registered as `mailOut` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bcc` | `` |  |  |
| `cc` | `` |  |  |
| `errorCatch` | `boolean` | `false` | Error Catch Port — Adds a dedicated "error" output port; a node failure emits msg.error={text,node,full} on it when wired, else the catch/problem-queue ladder applies |
| `from` | `` |  |  |
| `host` | `` |  |  |
| `html` | `boolean` | `false` | HTML Body — Send the payload as text/html instead of text/plain |
| `password` | `string` |  | Password — Optional SMTP-AUTH password (credential: masked, never exported) |
| `port` | `integer` | `0` | Port — SMTP port (0 = default for security: 465 tls, 587 starttls, 25 plain) |
| `security` | `string` | `starttls` | Security |
| `subject` | `` |  |  |
| `to` | `` |  |  |
| `username` | `string` |  | Username — Optional SMTP-AUTH username (credential: masked, never exported) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
