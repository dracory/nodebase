# UDP In node

Type: `udpIn`  
Category: `Network`

Receives UDP datagrams.

## Behavior

This node is registered as `udpIn` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `datatype` | `string` | `utf8` | Output — utf8: string payload; buffer: raw bytes; base64: base64-encoded string |
| `group` | `string` |  | Group — Multicast group to join (e.g. 225.0.18.83) |
| `iface` | `string` |  | Interface — Optional local interface name for the multicast join |
| `ipv` | `string` | `udp4` | Using |
| `multicast` | `string` | `false` | Listen For — false: plain UDP datagrams; true: multicast datagrams for Group |
| `port` | `integer` |  | Port — UDP port to listen on |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
