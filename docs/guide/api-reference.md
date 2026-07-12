# Public API

Everything you need is re-exported from the top-level `harel` package, so callers stay
decoupled from the internal module layout:

```text
from harel import (
    definition_from_dsl, definition_from_dsl_file, DslError,   # compile the DSL
    validate, validate_or_raise, Issue, ValidationError,       # static checks
    render,                                                     # PlantUML (mermaid: harel.viz.mermaid.render)
    Event, Execution,                                           # runtime data
    Driver, DurableRunner,                                      # runners
    ExecutionStore, DictStore, SqliteStore,                     # stores (durable extras: RedisStore, …)
    MachineResolver, DictResolver, FileResolver, ModuleResolver, SourceResolver, ResolveError,
)
```

## Compiling

```text
definition_from_dsl(text, name=None, *, base_path=None,
                    actions=None, guards=None, validate=False) -> Definition
definition_from_dsl_file(path, name=None, *, ...) -> Definition
```

- `name` selects the machine (required only if the file declares more than one).
- `actions={"handler": fn_or_dotted_path}` binds handlers; values may be callables or dotted
  paths. Wins over an in-DSL `bind`.
- `guards={"name": {"field__op": value}}` binds guards to predicate dicts. Wins over an in-DSL
  `guard`.
- `validate=True` raises `ValidationError` on any error-severity issue at build time.
- `base_path` is the directory `import`s resolve against (defaults to the file's directory for
  `_from_dsl_file`).

## Validating

```text
validate(defn) -> list[Issue]          # Issue(code, severity, path, message)
validate_or_raise(defn)                # raises ValidationError if any error-severity Issue
```

See [static validation](../tutorial/14-validation).

## Running (single process)

```text
DurableRunner(store, definitions, clock=time.time, resolver=None, trace=False)
    .create(definition_id, context=None, execution_id=None, priority=0) -> Execution
    .process(execution_id, event) -> Execution
    .fire_due_timers() -> int
    .cancel(execution_id, *, reason=None) -> Execution
    .terminate(execution_id) / .suspend(execution_id) / .resume(execution_id) -> Execution
```

- `definitions` is a `{definition_id: Definition}` registry; inline `invoke` targets register
  automatically.
- `clock` is injectable so [durable timers](../tutorial/07-timers) fire deterministically.
- `resolver` resolves submachine `invoke` FQNs not already in `definitions`.
- `trace=True` records the opt-in execution timeline in each commit (env `STM_TRACE`); off by
  default. See [stores](stores) and the [monitor](monitor).

## Running (distributed)

```text
from harel.engine.distributed import DistributedRunner
from harel.engine.transport import InMemoryTransport   # + Sqlite/Libsql/Redis/Postgres/Rqlite/Mongo/Sqs

DistributedRunner(store, transport, definitions, clock=time.time, resolver=None, trace=False)
    .create(definition_id, context=None, execution_id=None, priority=0) -> Execution
    .send(execution_id, event) -> None
    .worker(...) -> Worker        # .step() one message; .run(stop_event) loops
    .cancel / .terminate / .suspend / .resume
```

- `create` accepts an optional `execution_id` (use an externally-supplied id, e.g. a Stripe
  PaymentIntent id, instead of a generated one; raises `ExecutionAlreadyExists` if that id already
  exists — the create-or-find primitive) and a `priority` (0-4). Priority controls worker claim
  weighting under `DistributedRunner` (see [distribution](distribution)); on the single-process
  `DurableRunner` it is stored on the Execution but has no effect (no transport to weight).

See [distribution](distribution).

## Key data types

- **`Event(kind, data=None, id=None)`** — a `kind` (name) plus an optional JSON-serializable
  `data` payload.
- **`Execution`** — the serializable run state: `status`, `outcome`, `active_path`, `context`,
  `history`, `version`. Round-trips to/from JSON.
- **`Definition`** — the compiled, immutable machine (node tree + index). Built from the DSL;
  rendered, validated, and executed by reference.

## Resolvers (submachine `invoke`)

`MachineResolver` is the seam that maps an `invoke` FQN to a built `Definition`.
Inline `invoke { … }` targets and imported definitions resolve without one.

Four implementations ship out of the box:

| Class | Source |
| ----- | ------ |
| `DictResolver` | in-memory registry (pre-built `Definition` objects) |
| `FileResolver` | `.stm` file under a root directory (`a.b.c` → `root/a/b/c.stm`) |
| `ModuleResolver` | Python module attribute (`a.b.c` → `import a.b; mod.c`) |
| `SourceResolver` | injected `fqn → str` callable — databases, remote registries, etc. |

All four cache by FQN (each machine is compiled at most once). See the
[resolvers guide](resolvers) for usage examples and how to write a custom one.
