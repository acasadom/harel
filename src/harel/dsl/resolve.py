"""Source-building machine resolvers for `invoke` (the DSL side of the seam).

Each maps a logical FQN (`acme.jobs.worker`) to a built `Definition`, differing
only in where the *source* comes from — disk, a Python module, or an arbitrary
loader (e.g. a database). Convention: the **last FQN segment is the machine name**
(`acme.jobs.worker` -> a machine `worker`). Results are cached per FQN.

The in-memory `DictResolver` and the `MachineResolver` protocol live in
`harel.engine.resolve` (no DSL build step); these build via the DSL loader.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Callable, Optional

from harel.definition.model import Definition
from harel.dsl.loader import definition_from_dsl, definition_from_dsl_file
from harel.engine.resolve import ResolveError


def _machine_name(fqn: str) -> str:
    return fqn.split(".")[-1]


class _CachedResolver:
    """Base: cache built Definitions by FQN; subclasses implement `_build`."""

    def __init__(self) -> None:
        self._cache: dict[str, Definition] = {}

    def _build(self, fqn: str) -> Definition:  # pragma: no cover - overridden
        raise NotImplementedError

    def resolve(self, fqn: str) -> Definition:
        if fqn not in self._cache:
            self._cache[fqn] = self._build(fqn)
        return self._cache[fqn]


class FileResolver(_CachedResolver):
    """FQN -> a `.stm` file under one of `roots` (`a.b.c` -> `<root>/a/b/c.stm`)."""

    def __init__(self, roots: list[Path] | Path | str, suffix: str = ".stm") -> None:
        super().__init__()
        self._roots = [Path(roots)] if isinstance(roots, (str, Path)) else [Path(r) for r in roots]
        self._suffix = suffix

    def _build(self, fqn: str) -> Definition:
        rel = Path(*fqn.split(".")).with_suffix(self._suffix)
        for root in self._roots:
            path = root / rel
            if path.exists():
                return definition_from_dsl_file(path, _machine_name(fqn))
        raise ResolveError(f"no `.stm` for FQN {fqn!r} under {[str(r) for r in self._roots]}")


class ModuleResolver(_CachedResolver):
    """FQN -> a Python module attribute (`a.b.c` -> `import a.b; getattr(mod, 'c')`).
    The attribute may be a `Definition`, a `.stm` source string, or a zero-arg
    callable returning a `Definition`."""

    def _build(self, fqn: str) -> Definition:
        mod_path, _, attr = fqn.rpartition(".")
        if not mod_path:
            raise ResolveError(f"module FQN {fqn!r} needs a `module.attr` shape")
        try:
            obj = getattr(importlib.import_module(mod_path), attr)
        except (ImportError, AttributeError) as exc:
            raise ResolveError(f"cannot import machine {fqn!r}: {exc}") from exc
        if callable(obj) and not isinstance(obj, Definition):
            obj = obj()
        if isinstance(obj, Definition):
            return obj
        if isinstance(obj, str):
            return definition_from_dsl(obj, _machine_name(fqn))
        raise ResolveError(f"machine {fqn!r} is neither a Definition, source string, nor a builder")


class SourceResolver(_CachedResolver):
    """FQN -> `.stm` source via an injected loader (`fqn -> source | None`). The
    generic seam for a database (or any store): inject your query, get caching +
    building for free. A `None` (or missing) source raises `ResolveError`."""

    def __init__(self, load: Callable[[str], Optional[str]]) -> None:
        super().__init__()
        self._load = load

    def _build(self, fqn: str) -> Definition:
        source = self._load(fqn)
        if source is None:
            raise ResolveError(f"no source for FQN {fqn!r}")
        return definition_from_dsl(source, _machine_name(fqn))
