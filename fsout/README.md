# FS Out node

Type: `fsOut`  
Category: `Storage`

Writes the payload to a local/sftp/ftp directory locator (Switch-style send).

## Behavior

This node is registered as `fsOut` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `acceptAnyHostKey` | `boolean` | `false` | Accept Any Host Key — sftp locators: skip host-key verification (INSECURE — vulnerable to MITM; prefer pinning hostKey) |
| `appendNewline` | `boolean` | `false` | Append Newline — Append \n to every written payload |
| `createDir` | `boolean` | `false` | Create Directory — Create missing locator directories (mkdir -p) |
| `encoding` | `string` | `utf8` | Encoding — base64/hex decode a string payload before writing |
| `filename` | `string` |  | Filename — Base name to write; with filenameType "msg" the message's filename property is used, with "flow"/"global" the value is read from that context scope (message/context names are reduced to their base name) |
| `filenameType` | `string` | `fixed` | Filename Source |
| `hostKey` | `string` |  | Host Key — sftp locators: expected host public key (authorized_keys or base64); required unless acceptAnyHostKey is set |
| `keyFile` | `string` |  | SSH Key File — sftp locators: local path to an SSH private key |
| `location` | `string` |  | Location — Target directory locator: a local path, sftp://user@host/path or ftp://user@host/path. The file is written to location/filename |
| `overwriteFile` | `string` | `true` | If File Exists — true: overwrite via tmp+rename (atomic on local/sftp); false: append; delete: remove first then write fresh |
| `password` | `string` |  | Password — Remote locator password, or the keyFile passphrase (credential: masked, never exported) |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
