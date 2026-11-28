# DB node

Type: `db`  
Category: `Storage`

Parameterized SQL query/exec on a neat DSN (sqlite/postgres/mysql/...).

## Behavior

This node is registered as `db` and runs as part of a workflow. Its exact runtime behavior is implemented in `node.go`; this document summarizes the editor-facing purpose and configuration contract.

## Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `dsn` | `string` |  | DSN — Neat-style database DSN (sqlite://file.db, postgres://user:pass@host/db, mysql://...). Bare paths and :memory: are treated as SQLite. "env:NAME" resolves the DSN from the scoped env chain. Credential: masked, stored outside the exported flow document |
| `errorCatch` | `boolean` | `false` | Error Catch Port — Adds a dedicated "error" output port; a node failure emits msg.error={text,node,full} on it when wired, else the catch/problem-queue ladder applies |
| `operation` | `string` | `query` | Operation |
| `params` | `string` |  | Params Field — Optional dotted msg field path resolving to a bind-args array (e.g. "params"); empty uses the message's params data field |
| `readOnly` | `boolean` | `false` | Read Only — Reject exec operations at deploy time |
| `split` | `boolean` | `false` | Split Rows — Query only: emit one message per result row instead of one array payload |
| `sql` | `string` |  | SQL — Statement with ? bind placeholders (rebound to the driver's native style — $n for postgres, @pn for sqlserver). Params bind; values are never interpolated into the SQL string |

## Implementation

- `node.go` contains the executor, validation, schema, and registration.
- `editor.html` contains the editor integration.
- `node_test.go`, when present, contains behavioral tests.
