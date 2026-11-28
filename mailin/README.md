# Mail In node

Type: `mailIn`  
Category: `Network`

Watches an IMAP mailbox and emits one message per received mail.

## Behavior

This node is registered as `mailIn` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `delete` | `boolean` | `false` | Delete — Expunge delivered messages from the mailbox |
| `host` | `` |  |  |
| `mailbox` | `string` | `INBOX` | Mailbox |
| `markSeen` | `boolean` | `true` | Mark Seen — Set the \Seen flag on delivered messages (default on) |
| `password` | `string` |  | Password — Mailbox password (credential: masked, never exported) |
| `pollSeconds` | `integer` | `30` | Poll Interval (s) — Fallback poll interval in seconds; IDLE pushes wake the node sooner |
| `port` | `integer` | `0` | Port — IMAP port (0 = default for security: 993 tls, 143 starttls/plain) |
| `security` | `string` | `tls` | Security |
| `useIdle` | `boolean` | `true` | Use IDLE — Use IMAP IDLE between polls when the server supports it |
| `username` | `string` |  | Username — Mailbox username (credential: masked, never exported) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
