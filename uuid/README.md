# UUID node

The `uuid` node generates a fresh RFC 9562 UUID for every input message and writes it to a configured message property.

It is useful for correlation IDs, database keys, idempotency keys, file names, external API references, and other workflow-local identifiers.

## Behavior

- Exactly one new UUID is generated per executed input message.
- UUIDs are emitted in canonical lowercase string form, for example `018f3d2c-ca7b-7f42-9e11-65c8a4f1d2ab`.
- The input message object is preserved. Unrelated payload and metadata fields are left unchanged.
- The node uses Go's standard-library `uuid` package. It does not require a third-party UUID dependency.

## Configuration

| Field | Values | Default | Description |
| --- | --- | --- | --- |
| `version` | `v7`, `v4` | `v7` | UUID version to generate. `v7` is time-ordered and preferred for new identifiers. `v4` is random and useful for compatibility with systems that expect random UUIDs. |
| `target` | message property path | `payload` | Where the UUID string is written. Use `payload` to replace the payload, or a path such as `msg.identifiers.correlation` to write metadata. |

## Examples

With the default configuration:

```text
version: v7
target: payload
```

an input message with any payload produces an output message whose payload is a newly generated v7 UUID string.

With metadata output:

```text
version: v4
target: msg.identifiers.correlation
```

the original payload is unchanged and the generated v4 UUID is available at:

```text
msg.identifiers.correlation
```

## Validation

The node rejects unsupported versions and blank target property names during construction or execution.
