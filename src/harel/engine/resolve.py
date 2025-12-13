"""Machine resolution — the seam that maps a submachine **FQN** to a built
`Definition` for `invoke`.

A submachine is referenced black-box by a logical, dotted FQN (`acme.jobs.worker`)
recorded on `Node.invoke`; *how* that name loads is a separate, pluggable concern
(disk, Python module, a database, or an in-memory registry) — exactly like the
store/transport seams. Resolution is lazy (at the first spawn) and cached; an
unknown FQN raises `ResolveError`.

`DictResolver` (in-memory) lives here as it needs no DSL build step; the
source-building resolvers (file / module / arbitrary source loader) live in
`harel.dsl.resolve` since they call the DSL builder.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from harel.definition.model import Definition


class ResolveError(Exception):
    """Raised when a submachine FQN cannot be resolved to a `Definition`."""


@runtime_checkable
class MachineResolver(Protocol):
    """Maps a submachine FQN to a built `Definition`."""

    def resolve(self, fqn: str) -> Definition: ...


class DictResolver:
    """An in-memory registry: FQN -> already-built `Definition`. The simplest
    resolver — for tests and apps that build and register their machines up front."""

    def __init__(self, machines: dict[str, Definition]) -> None:
        self._machines = dict(machines)

    def register(self, fqn: str, defn: Definition) -> None:
        self._machines[fqn] = defn

    def resolve(self, fqn: str) -> Definition:
        if fqn not in self._machines:
            raise ResolveError(f"no machine registered for FQN {fqn!r}")
        return self._machines[fqn]
