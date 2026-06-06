"""Resolving a `Definition` for a listed Execution — the one piece of new policy the
monitor needs. The store persists `definition_id`/`definition_fqn` but not the Definition
object, so the TUI resolves it itself, reusing the engine's resolver seam
(`harel.engine.resolve.MachineResolver` + `harel.dsl.resolve.FileResolver`).

`DefinitionSource.get` never raises — an unresolvable id returns None, and the UI degrades
to a data-only view. Results are cached per key (resolution is the expensive part)."""

from __future__ import annotations

from typing import Optional

from harel.definition.model import Definition
from harel.engine.resolve import MachineResolver, ResolveError


class DefinitionSource:
    """Maps a (definition_id, optional FQN) to a `Definition` for the monitor. Backed by
    a preloaded `registry` (e.g. every machine under a directory, keyed by Definition.id —
    the worker's convention) and/or a `MachineResolver` for submachine FQNs. Caches hits
    AND misses so a missing definition isn't re-resolved on every poll."""

    def __init__(
        self,
        registry: Optional[dict[str, Definition]] = None,
        resolver: Optional[MachineResolver] = None,
    ) -> None:
        self._registry = dict(registry) if registry else {}
        self._resolver = resolver
        self._cache: dict[str, Optional[Definition]] = {}

    def get(self, definition_id: str, fqn: Optional[str] = None) -> Optional[Definition]:
        key = fqn or definition_id
        if key in self._cache:
            return self._cache[key]
        defn = self._registry.get(definition_id)
        if defn is None and self._resolver is not None:
            try:
                defn = self._resolver.resolve(fqn or definition_id)
            except (ResolveError, Exception):  # never let a resolution failure crash the UI
                defn = None
        self._cache[key] = defn
        return defn

    @classmethod
    def from_dir(cls, definitions_dir: str) -> "DefinitionSource":
        """Build a source from a directory of `.stm` files: a registry keyed by
        Definition.id (every machine, validated) plus a `FileResolver` so submachine
        FQNs (`a.b.c`) resolve too. Mirrors the worker's `load_definitions` convention."""
        from harel.dsl.resolve import FileResolver
        from harel.worker import load_definitions

        return cls(registry=load_definitions(definitions_dir), resolver=FileResolver(definitions_dir))

    @classmethod
    def empty(cls) -> "DefinitionSource":
        """A source that resolves nothing — the monitor then runs data-only."""
        return cls()
