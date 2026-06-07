"""Opt-in idempotency for side-effecting actions (the `B` approach).

The engine delivers events **at least once**: if a worker crashes after an action
ran but before the Execution commits, the event is redelivered and the action runs
again (dedupe is per *event*, not per *action* — `engine.store.processed_events`
catches a redelivery only when the prior attempt already committed). The driver
exposes a stable `stm.idempotency_key` per action so a side effect can be made
**effect-once** even across that window.

Why the dedupe MUST live in an external backend, not in harel's store/context:
the gap is a crash *before the commit*, so anything harel recorded (in the context
or the ExecutionStore) **rolls back with that failed commit** and is gone on the
retry. Only a record the *callee* owns — Redis `SET NX`, a DynamoDB conditional
put, Stripe's native idempotency key — survives, because it was written outside
harel's transaction. So `idempotent` takes a backend **you** supply.

    backend = DictIdempotency()                 # tests / single process
    actions = {"charge": idempotent(backend)(charge)}   # bind the wrapped action

`run_once(key, fn)` runs `fn` at most once per key and caches its result, so a
redelivery returns the same value (important for a selector that routes on it)
without repeating the side effect. Residual window: if the process dies between
`fn`'s external effect and the backend recording it, the effect can still repeat —
true exactly-once needs the effect and the claim to be atomic (an
idempotency-key-native service). The helper narrows the window; it does not
abolish physics.
"""

from __future__ import annotations

from typing import Any, Callable, Protocol, runtime_checkable


@runtime_checkable
class IdempotencyBackend(Protocol):
    def run_once(self, key: str, fn: Callable[[], Any]) -> Any:
        """Run `fn` at most once for `key`, returning its result (cached on repeat).
        Must be durable and atomic in production (e.g. Redis `SET NX` / a DynamoDB
        conditional put) — see this module's docstring on why an in-harel record
        would not survive the crash window."""
        ...


class DictIdempotency:
    """In-memory reference `IdempotencyBackend` for tests and single-process use.
    NOT durable — a real deployment supplies a backend over an external store."""

    def __init__(self) -> None:
        self._results: dict[str, Any] = {}

    def run_once(self, key: str, fn: Callable[[], Any]) -> Any:
        if key not in self._results:
            self._results[key] = fn()
        return self._results[key]


def idempotent(backend: IdempotencyBackend) -> Callable[[Callable], Callable]:
    """Wrap an action `(stm, event, **inputs)` so its body runs at most once per
    `stm.idempotency_key`, deduped through `backend`. With no key (e.g. a non-durable
    in-memory run) it runs normally — idempotency is a durable-execution concern."""

    def decorate(fn: Callable) -> Callable:
        def wrapper(stm: Any, event: Any, **inputs: Any) -> Any:
            key = getattr(stm, "idempotency_key", None)
            if key is None:
                return fn(stm, event, **inputs)
            return backend.run_once(key, lambda: fn(stm, event, **inputs))

        return wrapper

    return decorate
