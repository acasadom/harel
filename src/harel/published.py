"""Published machine versions: the store keeps every version of every machine, so an execution
finds the one it runs on whichever process picks it up, whatever files that process has.

A machine version is published from its `.stm` file — with every file it imports, transitively —
as a `DefinitionRecord`, and is never changed once stored: publishing it again with other content
is refused (`DefinitionConflict`). A worker publishes the machines it loads when it starts, so a
deploy that changed a machine without a new version stops there, before any execution meets it;
`harel publish DIR` does the same from a deploy pipeline.

A runner reads a version from the store when an execution needs one it hasn't loaded — or when
its own copy differs from the one the execution started on. The Definition it builds from the
record may call only the action modules the runner's own definitions call, plus those it is told
to allow (`action_modules=`, `HAREL_ACTION_MODULES`): a record in the store must not make a worker
run code its own machines don't.
"""

from __future__ import annotations

import os
import tempfile
import time
from importlib.util import resolve_name
from pathlib import Path
from typing import Iterable, Optional

from harel.definition.model import ActionRef, Definition
from harel.dsl import definition_from_dsl_file, machine_version, parse
from harel.engine.store import DefinitionRecord


class DefinitionConflict(ValueError):
    """A machine version is published already, with other content: a changed machine needs a
    new version."""


# --- bundling a file ---------------------------------------------------------------------------


def _imports(path: Path, seen: dict[Path, None]) -> None:
    """Add `path` and every file it imports, transitively, to `seen` (in order)."""
    if path in seen:
        return
    seen[path] = None
    for imported, _alias, _pos in parse(path.read_text()).imports:
        resolved = (path.parent / imported).resolve()
        if resolved.exists():
            _imports(resolved, seen)


def bundle(path: str | Path) -> dict:
    """The source of the machines of `path`: `{"main": <path>, "files": {<path>: <text>}}`, the
    file and every file it imports, by their paths relative to the directory that holds them all."""
    main = Path(path).resolve()
    seen: dict[Path, None] = {}
    _imports(main, seen)
    root = Path(os.path.commonpath([p.parent for p in seen]))
    return {
        "main": main.relative_to(root).as_posix(),
        "files": {p.relative_to(root).as_posix(): p.read_text() for p in seen},
    }


def definition_records(path: str | Path, now: Optional[float] = None) -> list[DefinitionRecord]:
    """One record per machine of `path`, at its declared version."""
    path = Path(path)
    text = path.read_text()
    source = bundle(path)
    published_at = time.time() if now is None else now
    records = []
    for name in parse(text).machines:
        version, digest = machine_version(text, name, base_path=path.parent)
        records.append(DefinitionRecord(name, version, digest, source, published_at))
    return records


def definition_from_record(
    record: DefinitionRecord, *, actions: Optional[dict] = None, guards: Optional[dict] = None
) -> Definition:
    """Build the Definition a record holds (its files, laid out in a scratch directory), binding
    handlers and guards as `definition_from_dsl` does with `actions` and `guards`: the record
    holds the DSL only, so what the code binds is the code's to give again."""
    with tempfile.TemporaryDirectory(prefix="harel-") as tmp:
        root = Path(tmp)
        for rel, text in record.source["files"].items():
            target = (root / rel).resolve()
            if not target.is_relative_to(root.resolve()):
                raise ValueError(f"machine {record.definition_id!r}: a file outside its bundle: {rel!r}")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
        defn = definition_from_dsl_file(
            root / record.source["main"], record.definition_id, actions=actions, guards=guards
        )
    if (defn.version, defn.digest) != (record.version, record.digest):
        raise ValueError(
            f"machine {record.definition_id!r} version {record.version}: its published source builds "
            f"version {defn.version}, digest {defn.digest}, not digest {record.digest}"
        )
    return defn


# --- publishing ----------------------------------------------------------------------------------


def read_definitions(definitions_dir: str | Path) -> list[DefinitionRecord]:
    """A record for every machine of every `*.stm` at the top of `definitions_dir`."""
    return [r for path in sorted(Path(definitions_dir).glob("*.stm")) for r in definition_records(path)]


def _conflict(record: DefinitionRecord, stored: DefinitionRecord) -> Optional[str]:
    if stored.digest == record.digest:
        return None
    return (
        f"machine {record.definition_id!r} version {record.version} is published with other content "
        f"(digest {stored.digest}, here {record.digest}): give the changed machine a new version"
    )


def _raise_conflicts(problems: list[str]) -> None:
    if problems:
        raise DefinitionConflict("; ".join(problems))


def publish_definitions(store, records: Iterable[DefinitionRecord]) -> None:
    """Publish `records` to `store`: each version not stored yet is stored, one stored with the
    same content is left as it is. Raises `DefinitionConflict` naming every version stored with
    other content (the others are published all the same)."""
    problems = []
    for record in records:
        problem = _conflict(record, store.publish_definition(record))
        if problem:
            problems.append(problem)
    _raise_conflicts(problems)


async def apublish_definitions(store, records: Iterable[DefinitionRecord]) -> None:
    """`publish_definitions` for an async store."""
    problems = []
    for record in records:
        problem = _conflict(record, await store.publish_definition(record))
        if problem:
            problems.append(problem)
    _raise_conflicts(problems)


# --- the action modules a Definition calls -------------------------------------------------------


def _action_refs(defn: Definition) -> Iterable[ActionRef]:
    for node in defn.index.values():
        for ref in (node.on_enter, node.on_activity, node.on_exit):
            if ref is not None:
                yield ref
        for t in node.transitions:
            if t.selector is not None:
                yield t.selector.action
    for sub in defn.submachines.values():
        yield from _action_refs(sub)


def action_modules(defn: Definition) -> set[str]:
    """The modules of the actions `defn` names by path (an action given as a callable has none)."""
    out = set()
    for ref in _action_refs(defn):
        if isinstance(ref.function, str):
            module = ref.function.rsplit(".", 1)[0]
            out.add(resolve_name(module, ref.package) if module.startswith(".") else module)
    return out


def allowed_action_modules(
    definitions: Iterable[Definition], extra: Iterable[str] = (), env: bool = True
) -> frozenset[str]:
    """What a Definition read from the store may call: the action modules of `definitions` (a
    runner's own), the modules or packages in `extra`, and those in `HAREL_ACTION_MODULES`
    (comma-separated) when `env`."""
    allowed = {m for d in definitions for m in action_modules(d)} | set(extra)
    if env:
        allowed |= {m.strip() for m in os.environ.get("HAREL_ACTION_MODULES", "").split(",") if m.strip()}
    return frozenset(allowed)


def check_action_modules(defn: Definition, allowed: frozenset[str]) -> None:
    """Raise `PermissionError` if `defn` names an action in a module outside `allowed` (a module
    there allows itself and every module under it)."""
    refused = sorted(
        m for m in action_modules(defn) if not any(m == a or m.startswith(a + ".") for a in allowed)
    )
    if refused:
        raise PermissionError(
            f"machine {defn.id!r} version {defn.version} calls actions in {', '.join(refused)}, which "
            f"this runner doesn't allow (allow them with action_modules= or HAREL_ACTION_MODULES)"
        )


def check_actions_importable(defn: Definition) -> None:
    """Raise `ImportError` if an action `defn` names by path can't be imported here — a version
    published by a newer deploy, met by a process still running the older code."""
    from harel.engine.runtime import _resolve

    for ref in _action_refs(defn):
        if isinstance(ref.function, str):
            try:
                _resolve(ref)
            except (ImportError, AttributeError) as exc:
                raise ImportError(
                    f"machine {defn.id!r} version {defn.version} calls {ref.function}, which this "
                    f"process can't import ({exc})"
                ) from exc


class PublishedDefinitions:
    """A runner's view of the published versions: builds a record into a Definition once and
    keeps it (a published version never changes), binding the runner's `actions` and `guards`,
    checking its action modules against `allowed` — to which the modules of the paths in
    `actions` are added: what the runner binds itself is its own code — and that this process
    can import every action it names. `get` is what it has
    built of a version, without asking the store again."""

    def __init__(
        self, allowed: frozenset[str], *, actions: Optional[dict] = None, guards: Optional[dict] = None
    ) -> None:
        own = {v.rsplit(".", 1)[0] for v in (actions or {}).values() if isinstance(v, str) and "." in v}
        self.allowed = allowed | own
        self.actions, self.guards = actions, guards
        self._built: dict[tuple[str, int], Definition] = {}
        self.warned: set[tuple[str, int]] = set()  # versions whose local copy differs, logged once

    def get(self, definition_id: str, version: int) -> Optional[Definition]:
        return self._built.get((definition_id, version))

    def build(self, record: DefinitionRecord) -> Definition:
        key = (record.definition_id, record.version)
        if key not in self._built:
            defn = definition_from_record(record, actions=self.actions, guards=self.guards)
            check_action_modules(defn, self.allowed)
            check_actions_importable(defn)
            self._built[key] = defn
        return self._built[key]
