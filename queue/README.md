# Queue node

Type: `queue`  
Category: `Queue`

Durable, inspectable staging queue with a hold/release gate.

## Behavior

This node is registered as `queue` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `backend` | `string` | `durable` | Storage Backend — durable: the configured durable inbox store (shared, multi-runner). dir: one file per queued item in a directory, inspectable with OS tools |
| `capacity` | `number` | `0` | Capacity — Maximum queued items (0 = unbounded); alias for inboxCapacity |
| `dir` | `string` |  | Directory — Directory locator the dir backend writes one {seq}-{msgid}.json file per item into: a local path (or NFS/SMB/SSHFS mount), or sftp://user@host/path or ftp://user@host/path. Remote backends are POLLING (no fsnotify — the drainer scans on arm). Multi-runner safety is a deployment property, the node makes no claim. |
| `hold` | `boolean` | `false` | Hold — Start held: deliveries enqueue but nothing drains until released |
| `hostKey` | `string` |  | Host Key — sftp locators: expected host public key (authorized_keys or base64); empty accepts any key (insecure) |
| `keyFile` | `string` |  | SSH Key File — sftp locators: local path to an SSH private key; password doubles as the passphrase |
| `password` | `string` |  | Password — Remote locator password, or the keyFile passphrase (credential: masked, never exported) |
| `policy` | `string` | `block` | Overflow Policy — What happens when the queue reaches capacity |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
