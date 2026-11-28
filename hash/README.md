# Hash node

Type: `hash`  
Category: `Transform`

Hashes a whole message property with sha256, sha1, sha512, or legacy md5.

## Behavior

This node is registered as `hash` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `algorithm` | `string` | `sha256` | Algorithm — md5 is legacy and is not collision-resistant; prefer sha256 or sha512 for security-sensitive use |
| `encoding` | `string` | `hex` | Output Encoding |
| `source` | `string` | `payload` | Source Property — Property to hash as one whole byte sequence |
| `target` | `string` | `msg.hash` | Target Property — Where the encoded hash is written; use payload to replace the payload or msg.* for metadata/attribute-style output |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
