# Checkpoint node

Type: `checkpoint`  
Category: `Queue`

Holds each message until a human releases it via a named route.

## Behavior

This node is registered as `checkpoint` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `backend` | `string` | `durable` | Storage Backend — durable: the configured durable inbox store (shared, multi-runner). dir: one file per held item in a directory, inspectable with OS tools |
| `capacity` | `number` | `0` | Capacity — Maximum held items (0 = unbounded); alias for inboxCapacity |
| `dir` | `string` |  | Directory — Directory the dir backend writes one {seq}-{msgid}.json file per item into. May be a local path or an NFS/SMB/SSHFS mount; multi-runner safety is a deployment property, the node makes no claim. |
| `policy` | `string` | `block` | Overflow Policy — What happens when the checkpoint reaches capacity |
| `routes` | `array` | `[approve decline]` | Routes — Ordered list of route labels; routes[i] labels output_{i+1}. Each route becomes a button in the inspection panel — releasing an item via a route delivers it only on that edge |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
