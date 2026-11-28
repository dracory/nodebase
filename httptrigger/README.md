# HTTP Trigger node

Type: `httpTrigger`  
Category: `Triggers`

Starts a workflow with an HTTP request.

## Behavior

This node is registered as `httpTrigger` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `cors` | `` |  | CORS — Accepted for Node-RED compatibility; currently ignored at runtime |
| `listener` | `string` | `own` | Listener — own: bind a private listener on Port. main: serve Path on the admin server's port (Node-RED style) — behind admin auth when configured; Port is ignored |
| `method` | `string` | `GET` | HTTP method the route answers (case-insensitive — Node-RED stores lowercase) |
| `path` | `string` | `/trigger` | URL path (":" segments become route params). listener "main": avoid /api, /editor, /dashboard and other admin prefixes — they are reserved |
| `port` | `integer` | `8081` | Port for the own listener (must differ from the admin port; ignored when listener is "main") |
| `responseTimeout` | `number` | `0` | Response Timeout (s) — Seconds to wait for a downstream httpResponse before a 504 (0 = 15s default) |
| `swaggerDoc` | `object` |  | Swagger Doc — Accepted for Node-RED compatibility; currently ignored at runtime |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
