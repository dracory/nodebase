# Submit node

Type: `submitIn`  
Category: `Triggers`

Receives uploaded files via a web submit point and stamps the submitting user into the message.

## Behavior

This node is registered as `submitIn` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `auth` | `string` | `none` | Authentication — admin: require HTTP Basic credentials from the admin auth file (the consuming application's auth-file env var); the authenticated username lands in msg.submit.submittedBy |
| `listener` | `string` | `own` | Listener — own: bind a private listener on Port. main: serve Path on the admin server's port — behind admin auth when configured; Port is ignored |
| `maxSize` | `integer` | `0` | Max Upload Size (bytes) — Largest accepted upload; oversized requests are rejected with 413 (0 = 10 MiB default) |
| `path` | `string` | `/submit` | Path — URL path accepting uploads (POST). ":name" segments become route params in msg.submit.params |
| `port` | `integer` | `8081` | Port for the upload listener (must differ from the admin port; ignored when listener is "main") |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
