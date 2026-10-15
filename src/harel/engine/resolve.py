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

from typing import Iterable, Optional, Protocol, runtime_checkable

from harel.definition.model import Definition


class ResolveError(Exception):
    """Raised when a submachine FQN cannot be resolved to a `Definition`."""


class DefinitionNotLoaded(KeyError):
    """An execution runs a machine version this runner hasn't loaded. Not the execution's
    fault — during a rolling deploy another worker may have it — so a worker retries the
    message rather than failing the execution."""

    def __str__(self) -> str:
        return str(self.args[0]) if self.args else ""


# A registry maps a machine's name to its highest version, and `name@version` to each version.


def register_definition(definitions: dict[str, Definition], defn: Definition) -> None:
    """Add `defn` to a registry under `id@version`, and under `id` when it is the highest
    version there."""
    definitions[f"{defn.id}@{defn.version}"] = defn
    current = definitions.get(defn.id)
    if current is None or current.version < defn.version:
        definitions[defn.id] = defn


def definition_registry(definitions: Iterable[Definition]) -> dict[str, Definition]:
    """The registry a runner takes, from every version of every machine to run: a new
    execution gets a machine's highest version, an existing one keeps its own. The same
    version of a machine twice with different content (digest) is refused."""
    out: dict[str, Definition] = {}
    for defn in definitions:
        prev = out.get(f"{defn.id}@{defn.version}")
        if prev is not None and prev is not defn and prev.digest != defn.digest:
            raise ValueError(
                f"machine {defn.id!r} version {defn.version} is loaded twice with different "
                f"content: give the changed one a new version"
            )
        register_definition(out, defn)
    return out


def find_definition(definitions: dict[str, Definition], name: str, version: int) -> Optional[Definition]:
    """Version `version` of machine `name` in a registry, or None."""
    defn = definitions.get(f"{name}@{version}")
    if defn is None:
        defn = definitions.get(name)
        if defn is not None and defn.version != version:
            return None
    return defn


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
