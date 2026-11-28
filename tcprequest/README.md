# TCP Request node

Type: `tcpRequest`  
Category: `Network`

Sends the payload over TCP and emits the server's reply.

## Behavior

This node is registered as `tcpRequest` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `delimiter` | `string` |  | Delimiter — Reply terminator for delimiter mode (escape sequences like \n allowed) |
| `errorCatch` | `boolean` | `false` | Error Catch Port — Adds a dedicated "error" output port; a node failure emits msg.error={text,node,full} on it when wired, else the catch/problem-queue ladder applies |
| `host` | `` |  |  |
| `keep` | `boolean` | `false` | Keep Connection — Reuse the client connection across messages instead of connect-send-close per message |
| `length` | `integer` |  | Length — Fixed reply size for length mode |
| `mode` | `string` |  | Reply Framing (legacy) — Legacy framing selector (timeout\|delimiter\|length); ignored when Close (out) is set |
| `newline` | `string` |  | Reply Delimiter — In sit mode with a string return, replies split on this delimiter (escape sequences like \n allowed) |
| `out` | `string` |  | Close — Node-RED reply selector: time = after a timeout of Splitc ms; char = after the Splitc character; count = after Splitc characters; sit = never close (stream replies); immed = immediately, don't wait |
| `port` | `integer` |  | Port — Remote port; the message's port metadata overrides it |
| `ret` | `string` | `string` | Return Type — string: reply payload as text; buffer: raw bytes |
| `server` | `string` |  | Server — Remote host to connect to (Node-RED field name; host is the legacy alias) |
| `splitc` | `string` |  | Splitc — Node-RED close parameter for time/char/count: milliseconds, delimiter character or byte count |
| `timeout` | `number` | `10` | Timeout (seconds) — Bounds connect + reply wait; 0 = 10 seconds |
| `tls` | `string` |  | TLS Config — Optional tlsConfig node; the connection is upgraded to TLS with that material |
| `trim` | `boolean` | `false` | Re-attach Delimiter — Keep the delimiter attached to each reply part in sit mode |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
