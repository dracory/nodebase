# nodebase

Engine-agnostic node executors for nodestore-based workflows — the
built-in Node-RED-style pack (inject, function, switch, http, tcp/udp,
mqtt, file/fs, db, queue, split/join, …) plus the `NodeHost` contract
that lets any runner host them and anyone write compatible packs.

## Why a separate package

Nodes are the shared vocabulary between storage and runtime:

- `nodestore` is pure storage — it does not know how flows are
  executed or admined.
- `noderun` is one runner — the reference engine.
- `nodebase` sits between them: executors program against `nodestore`
  types (`NodeInterface`, `MessageInterface`) and a narrow `NodeHost`
  interface defined here. Any runner that implements `NodeHost` can
  host this pack — and any pack written against `NodeHost` runs on
  `noderun`.

Dependency direction: `noderun → nodebase → nodestore`. Nothing here
imports a concrete engine.

## The contracts

- `NodeExecutor` behavior: `ID()`, `Type()`, `Execute(ctx, msg)`,
  `Validate()` — satisfied structurally; executors embed
  `nodestore.NodeInterface` so identity/type/Data live on the node
  entity.
- `NodeHost` — the ~20-method runtime seam executors call: emission
  (`Emit`, `EmitEvent`, `InjectMessage`, `ResolveResponse`,
  `DiscardPending`), run state (`GetOrCreateNodeState`), host services
  (`HTTPRouter`, `FileRoot`, `ResolvedContextStore`, `NodeEnv`,
  `LoadTLSConfig`, `RunContext`, `RunningFlow`), and lifecycle.
- `NodeFactory(node, host)` — construction signature; the engine
  passes itself in as the host.
- Optional capability interfaces a runner may check: `PortRouter`,
  `MessageExpander`, `Stopper`, `Activator`, `RunEventer`, inject
  firing.
- `BaseNode` — the embeddable executor-side shell (identity, type,
  Data delegation to the node entity) living here, not in any engine.

## Layout — folder per node

Following Node-RED, each node type is a self-contained folder:

```
debug/
  node.go       // executor + factory, self-registers via init()
  editor.html   // config dialog template + help text (go:embed)
  schema.json   // optional, if not inline in node.go
```

Add a node = add a folder; remove a node = remove a folder. Shared
helpers live at the package root or in `internal/`.

## Writing a pack

Implement the contracts above, register via the engine's
`RegisterNodeType(typeID, factory)` (duplicate typeID is an error),
and carry the editor descriptor (name/icon/category/schema) with your
registration so the admin palette serves it. A workflow referencing
unregistered types fails loudly at deploy.

## Rules

- Executors never resolve engine state inside `Stop()` (deadlock —
  stop holds the run mutex); resolve eagerly in `Execute` and cache
  the state pointer on the executor shell.
- Imports `nodestore` only.
