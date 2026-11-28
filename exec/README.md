# Exec node

Type: `exec`  
Category: `Actions`

Runs a system command (requires the host to allow exec).

## Behavior

This node is registered as `exec` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `addpay` | `` |  |  |
| `append` | `` |  |  |
| `command` | `` |  |  |
| `errorCatch` | `boolean` | `false` | Error Catch Port — Adds a dedicated "error" output port; a node failure emits msg.error={text,node,full} on it when wired, else the catch/problem-queue ladder applies |
| `oldrc` | `boolean` | `false` | Legacy Return Code — Emit the bare exit code as the third-output payload instead of {code:...} |
| `timer` | `number` | `0` | Kill Timeout (seconds) — Kill the command after this many seconds; 0 = no timeout |
| `useSpawn` | `boolean` | `false` | Spawn Mode — Spawn the process and stream stdout/stderr per chunk instead of buffering |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
