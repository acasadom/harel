# Machine resolvers

When a state uses `invoke pkg.machines.payment`, the engine needs to turn that
dotted string into a compiled `Definition`. A **resolver** is the seam that does
that lookup — it is called lazily (at the first spawn) and its result is cached,
so each FQN is built at most once.

Inline `invoke { … }` targets and imported definitions resolve without one. A
resolver is only needed when the machine is named by an external FQN.

## The four resolvers

The examples on this page share a common setup:

```python
import tempfile
from pathlib import Path
from harel import definition_from_dsl, DictStore, DurableRunner
from harel.engine.resolve import DictResolver, MachineResolver, ResolveError

PAYMENT_SRC = """
event Paid {}
machine payment {
  initial Pending
  state Pending {}
  final Done success {}
  from Pending to Done on Paid
}
"""
REFUND_SRC = """
event Refunded {}
machine refund {
  initial Pending
  state Pending {}
  final Done success {}
  from Pending to Done on Refunded
}
"""
ORDER_SRC = """
machine order {
  initial Cart
  state Cart {}
  final Done success {}
  from Cart to Done
}
"""

payment_defn = definition_from_dsl(PAYMENT_SRC, "payment")
refund_defn  = definition_from_dsl(REFUND_SRC,  "refund")
order_defn   = definition_from_dsl(ORDER_SRC,   "order")
store        = DictStore()
```

### `DictResolver`

An in-memory registry: you build the `Definition` objects yourself and hand them
in. The simplest option — good for tests and for apps that load all their
machines at start-up.

```python
resolver = DictResolver({
    "acme.payments.payment": payment_defn,
    "acme.payments.refund":  refund_defn,
})
runner = DurableRunner(store, {order_defn.id: order_defn}, resolver=resolver)
```

`DictResolver.register(fqn, defn)` adds entries after construction.

---

### `FileResolver`

Maps `a.b.c` → `<root>/a/b/c.stm`. Compiles and caches on first use.
Accepts a single root or a list of roots (searched in order).

```python
from harel.dsl.resolve import FileResolver

# write the .stm files into a temp directory that mirrors the FQN hierarchy
_tmp = Path(tempfile.mkdtemp())
(_tmp / "acme" / "payments").mkdir(parents=True)
(_tmp / "acme" / "payments" / "payment.stm").write_text(PAYMENT_SRC)
(_tmp / "acme" / "payments" / "refund.stm").write_text(REFUND_SRC)

resolver = FileResolver(_tmp)
# multiple roots — first match wins:
# resolver = FileResolver([_tmp, Path("vendor/machines/")])

runner = DurableRunner(store, {order_defn.id: order_defn}, resolver=resolver)
```

`acme.payments.payment` resolves to `<root>/acme/payments/payment.stm`.
The last segment of the FQN is the machine name used during compilation.

Use this when machines live in a directory tree that mirrors the FQN hierarchy —
the common case for a service that owns its own `.stm` files.

---

### `ModuleResolver`

Maps `a.b.c` → `import a.b; getattr(mod, "c")`. The attribute can be:

- a `Definition` (already built)
- a `.stm` source string (compiled on first resolve)
- a zero-arg callable that returns a `Definition`

```python
# docs-test: skip
from harel.dsl.resolve import ModuleResolver

resolver = ModuleResolver()
runner = DurableRunner(store, {order_defn.id: order_defn}, resolver=resolver)
```

The matching Python module would expose a `payment` attribute:

```python
# docs-test: skip  — illustrative: this lives in acme/payments.py
from harel import definition_from_dsl_file
from pathlib import Path

# attribute name = last FQN segment ("payment")
payment = definition_from_dsl_file(Path(__file__).parent / "payment.stm", "payment")
```

`acme.payments.payment` → `import acme.payments` → `acme.payments.payment`.

Use this when you distribute machines as Python packages, or when the build
step belongs with the module rather than at runner start-up.

---

### `SourceResolver`

The generic seam: you inject a callable `fqn → str | None`; the resolver
compiles and caches whatever source it returns, raising `ResolveError` for
`None`.

```python
from harel.dsl.resolve import SourceResolver

# in a real app: load source from a database or remote registry
_db: dict[str, str] = {
    "acme.payments.payment": PAYMENT_SRC,
    "acme.payments.refund":  REFUND_SRC,
}

resolver = SourceResolver(_db.get)
runner = DurableRunner(store, {order_defn.id: order_defn}, resolver=resolver)
```

Use this when machines are stored outside the filesystem — a database, a remote
registry, an object store — or when the lookup logic is too custom for the other
resolvers.

---

## Passing a resolver to the runner

Both `DurableRunner` and `DistributedRunner` accept an optional `resolver`
parameter. The `definitions` dict is checked first; the resolver is the fallback
for FQNs not already registered there.

```python
from harel.engine.distributed import DistributedRunner
from harel.engine.transport.sqlite import SqliteTransport

runner = DistributedRunner(
    store,
    SqliteTransport(),
    definitions={order_defn.id: order_defn},  # top-level machine — always registered
    resolver=FileResolver(_tmp),               # resolves any invoke FQN on demand
)
```

The worker inherits the resolver automatically via `runner.worker(...)`.

## Writing a custom resolver

Implement the `MachineResolver` protocol — one method:

```python
from harel.definition.model import Definition

class MyResolver:
    def resolve(self, fqn: str) -> Definition:
        defn = _db.get(fqn)  # _db maps fqn → already-built Definition
        if defn is None:
            raise ResolveError(f"unknown machine {fqn!r}")
        return defn

_db = {"acme.payments.payment": payment_defn}
assert isinstance(MyResolver(), MachineResolver)  # runtime_checkable Protocol
```

`MachineResolver` is a `runtime_checkable` `Protocol`, so no inheritance is
required — duck typing is enough.
