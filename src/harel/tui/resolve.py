"""Resolving a `Definition` for a listed Execution — the one piece of new policy the
monitor needs. The store persists `definition_id`/`definition_fqn` but not the Definition
object, so the TUI resolves it itself, reusing the engine's resolver seam
(`harel.engine.resolve.MachineResolver` + `harel.dsl.resolve.FileResolver`).

`DefinitionSource.get` never raises — an unresolvable id returns None, and the UI degrades
to a data-only view. Results are cached per key (resolution is the expensive part)."""

from __future__ import annotations

from typing import Optional

from harel.definition.model import Definition
from harel.engine.resolve import MachineResolver, ResolveError, find_definition


class DefinitionSource:
    """Maps a (definition_id, optional FQN, version) to a `Definition` for the monitor. Backed
    by a preloaded `registry` (e.g. every machine under a directory, keyed by Definition.id
    and `id@version` — the worker's convention) and/or a `MachineResolver` for submachine
    FQNs. Caches hits AND misses so a missing definition isn't re-resolved on every poll."""

    def __init__(
        self,
        registry: Optional[dict[str, Definition]] = None,
        resolver: Optional[MachineResolver] = None,
        sources: Optional[dict[str, str]] = None,
    ) -> None:
        self._registry = dict(registry) if registry else {}
        self._resolver = resolver
        # "definition_id@version" (or a bare definition_id) -> .stm source text
        self._sources = dict(sources) if sources else {}
        self._cache: dict[str, Optional[Definition]] = {}

    def source(self, definition_id: str, version: int = 1) -> Optional[str]:
        """The `.stm` source text of that version of the machine (None if unknown). For the
        monitor's collapsible source view; only populated when built from a directory."""
        return self._sources.get(f"{definition_id}@{version}", self._sources.get(definition_id))

    def get(self, definition_id: str, fqn: Optional[str] = None, version: int = 1) -> Optional[Definition]:
        key = f"{fqn or definition_id}@{version}"
        if key in self._cache:
            return self._cache[key]
        defn = find_definition(self._registry, definition_id, version)
        if defn is None and self._resolver is not None:
            try:
                defn = self._resolver.resolve(fqn or definition_id)
            except (ResolveError, Exception):  # never let a resolution failure crash the UI
                defn = None
            if defn is not None and defn.version != version:
                defn = None
        self._cache[key] = defn
        return defn

    @classmethod
    def from_dir(cls, definitions_dir: str) -> "DefinitionSource":
        """Build a source from a directory of `.stm` files: a registry keyed by
        Definition.id (every machine, validated) plus a `FileResolver` so submachine
        FQNs (`a.b.c`) resolve too. Mirrors the worker's `load_definitions` convention."""
        from pathlib import Path

        from harel.dsl.parser import parse
        from harel.dsl.resolve import FileResolver
        from harel.worker import load_definitions

        # map each machine version to its file's source text, for the monitor's collapsible
        # DSL view.
        sources: dict[str, str] = {}
        for path in sorted(Path(definitions_dir).glob("*.stm")):
            text = path.read_text()
            prog = parse(text)
            for name in prog.machines:
                sources[f"{name}@{prog.versions.get(name, 1)}"] = text
        return cls(
            registry=load_definitions(definitions_dir),
            resolver=FileResolver(definitions_dir),
            sources=sources,
        )

    @classmethod
    def empty(cls) -> "DefinitionSource":
        """A source that resolves nothing — the monitor then runs data-only."""
        return cls()
