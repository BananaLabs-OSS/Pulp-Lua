# Pulp-Lua

Pulp-Lua is a sandboxed Lua application-orchestration cell for Pulp.

WASM cells remain the stateful and performance-critical engines. Lua supplies
the application-specific workflow that composes those engines:

```text
event -> Lua handler -> sibling engine calls -> commands/events -> caller
```

The Lua cell does not share linear memory with sibling cells. It communicates
through Pulp's manifest-authorized sibling ABI. This keeps language boundaries
explicit and preserves per-cell memory and capability isolation.

## Lua API

```lua
pulp.on(event_name, handler)
pulp.call(cell_name, function_name, payload_table)
pulp.call_raw(cell_name, function_name, byte_string)
pulp.command(command_name, payload_table)
pulp.emit(event_name, payload_table)
pulp.state_get(key)
pulp.state_set(key, value)
pulp.log(message)
```

Handlers receive one payload and may return any MessagePack-compatible value.
`pulp.command` and `pulp.emit` append declarative effects to the dispatch
result. The caller applies commands and routes events after the Lua sibling
call has returned, avoiding forbidden synchronous `A -> B -> A` cycles.

`pulp.call` is the structured, JSON-shaped path: nil, booleans, strings,
finite numbers, string-keyed objects, and arrays. Because Lua numbers use
IEEE-754 doubles, integer inputs must be within
`-9007199254740991..9007199254740991`. Encode larger IDs and counters as
strings. Lua's empty table is ambiguous, so an empty table returned from Lua is
encoded as an object rather than an array.

Use `pulp.call_raw` when a sibling contract requires exact binary data or a
typed MessagePack envelope. It forwards the supplied Lua byte string without
decoding it, and returns the sibling response as a byte string. This is the
appropriate boundary for typed engine protocols that must preserve integer
widths, binary fields, or other MessagePack distinctions.

`pulp.pack` produces deterministic MessagePack: every string-keyed Lua object,
including objects nested in arrays or other objects, is encoded with keys in
lexical order. Semantically identical values therefore produce identical
bytes regardless of table insertion order or map iteration order. Contiguous
integer keys starting at 1 are arrays; all other table-shaped objects require
string keys. `pulp.raw` deliberately embeds one already-validated MessagePack
value verbatim, so canonicalization does not rewrite bytes inside that value.

Only base, table, string, and deterministic math functions are available.
Filesystem, OS, package loading, debug access, dynamic source loading, and
uncontrolled randomness are not exposed.

## Manifest wiring

The shipped manifest contains a self-contained health handler. An application
manifest must declare every engine the script may call:

```toml
provides = ["orchestrator"]
consumes = ["sessions", "physics", "renderer"]
depends_on = ["sessions", "physics", "renderer"]
```

`consumes` is the permission boundary; Lua cannot bypass it by constructing a
cell name dynamically.

## Dispatch wire

Call the cell's `orchestrator.dispatch` sibling function with MessagePack:

```go
type DispatchRequest struct {
    Event   string `msgpack:"event"`
    Payload any    `msgpack:"payload,omitempty"`
}
```

The response contains `value`, `commands`, and `events`.
