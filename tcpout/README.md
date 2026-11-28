# TCP Out node

Type: `tcpOut`  
Category: `Network`

Sends data over TCP, or replies on the incoming session.

## Behavior

This node is registered as `tcpOut` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `base64` | `boolean` | `false` | Decode Base64 — Decode a base64 payload string before sending |
| `beserver` | `string` | `client` | Type — client: connect to host:port and keep the connection; server: listen on port and write to every connected client; reply: answer the tcpIn session that produced the message |
| `end` | `boolean` | `false` | Close Connection — Close the connection after each write (client and server types) |
| `host` | `string` |  | Host — Target host (default 127.0.0.1) |
| `port` | `integer` |  | Port — Target port (client) or listen port (server); unused for reply |
| `reply` | `boolean` |  | Reply to Session (legacy) — Legacy alias of Type=reply; ignored when Type is set |
| `tls` | `string` |  | TLS Config — Optional tlsConfig node; client-mode dials are upgraded to TLS with that material |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
