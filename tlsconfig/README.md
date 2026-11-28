# TLS Config node

Type: `tlsConfig`  
Category: `Network`

Shared TLS material (cert/key/CA) referenced by tcp nodes.

## Behavior

This node is registered as `tlsConfig` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `ca` | `` |  |  |
| `cert` | `` |  |  |
| `key` | `` |  |  |
| `servername` | `` |  |  |
| `verifyservercert` | `boolean` | `true` | Verify Peer Certificate — false disables peer verification (insecure; Node-RED verifyservercert) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
