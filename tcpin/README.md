# TCP In node

Type: `tcpIn`  
Category: `Network`

Receives raw TCP data (listen or connect).

## Behavior

This node is registered as `tcpIn` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `datamode` | `string` | `stream` | Output — stream: emit each chunk / delimited part; single: one message with the whole buffer at close; length: emit every Length bytes; timeout: flush the buffer after FlushMS ms of silence |
| `datatype` | `string` | `utf8` | Payload — utf8: string payload; buffer: raw bytes; base64: base64-encoded string |
| `flushms` | `integer` |  | Flush (ms) — Idle interval before the buffered data flushes in timeout mode |
| `host` | `string` |  | Host — Remote host for connect mode (default 127.0.0.1) |
| `length` | `integer` |  | Length — Chunk size for length mode |
| `mode` | `string` |  | Mode — Legacy alias of Type (listen=server, connect=client); ignored when Type is set |
| `newline` | `string` |  | Delimiter — Stream delimiter for utf8 payloads (escape sequences like \n allowed; empty = emit each chunk) |
| `port` | `integer` |  | Port — Listen port (listen mode) or remote port (connect mode) |
| `server` | `string` | `server` | Type — server: accept connections on port; client: dial out to host:port (Node-RED 'server' field) |
| `tls` | `string` |  | TLS Config — Optional tlsConfig node; connect-mode dials are upgraded to TLS with that material |
| `topic` | `string` |  | Topic — Stamped on emitted messages as metadata topic |
| `trim` | `boolean` | `false` | Re-attach Delimiter — Keep the delimiter attached to each emitted part |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
