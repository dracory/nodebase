# FS In node

Type: `fsIn`  
Category: `Storage`

Polls a local/sftp/ftp directory locator and emits one message per new file.

## Behavior

This node is registered as `fsIn` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `acceptAnyHostKey` | `boolean` | `false` | Accept Any Host Key — sftp locators: skip host-key verification (INSECURE — vulnerable to MITM; prefer pinning hostKey) |
| `delete` | `boolean` | `false` | Delete After Emit — Remove each file after it is emitted (Switch receive-and-consume) |
| `format` | `string` | `utf8` | Format — utf8: payload = file contents as string; binary: payload = []byte; path: payload = the locator path only (pairs with a downstream reader) |
| `hostKey` | `string` |  | Host Key — sftp locators: expected host public key (authorized_keys or base64); required unless acceptAnyHostKey is set |
| `interval` | `number` | `5` | Poll Interval (s) — Seconds between directory polls |
| `keyFile` | `string` |  | SSH Key File — sftp locators: local path to an SSH private key |
| `location` | `string` |  | Location — Directory locator to poll: a local path, sftp://user@host/path or ftp://user@host/path. Remote polling only — no fsnotify |
| `password` | `string` |  | Password — Remote locator password, or the keyFile passphrase (credential: masked, never exported) |
| `pattern` | `string` |  | Pattern — Optional glob filter on file base names (e.g. "*.xml"); empty accepts every file |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
