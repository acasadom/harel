"""The monitor's read model — a pure facade over an `ExecutionStore` plus a
`DefinitionSource`. No textual, no async, no timers of its own: it snapshots the store
and builds the renderable data the UI displays, reading one execution by its id. The textual
app calls these from worker threads so a slow store never blocks the UI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from harel.definition.model import Definition
from harel.engine.execution import Execution, ExecutionPage, Status
from harel.engine.store import ExecutionStore, OutboxEntry, PendingWork, SpawnEntry
from harel.tui.resolve import DefinitionSource
from harel.tui.trace import TraceStep
from harel.tui.tree import TreeModel, build_tree_model


@dataclass(frozen=True)
class ChildRow:
    """A child execution the detail lists: an orthogonal region or an invoked submachine, with
    the join state its parent keeps for it."""

    id: str
    kind: str  # "region" | "invoke"
    root_path: str  # the region's branch ("" for an invoked machine)
    finished: bool
    outcome: Optional[str]


@dataclass
class ExecutionDetail:
    """Everything the detail screen shows for one Execution: the full record, the
    statechart highlight tree, what is waiting for it, its children and its timeline."""

    execution: Execution
    tree: TreeModel
    pending: PendingWork = field(default_factory=PendingWork)  # timers, inbound events, spawns
    children: list[ChildRow] = field(default_factory=list)
    trace: list[TraceStep] = field(default_factory=list)  # the execution timeline (HAREL_TRACE)
    source: Optional[str] = None  # the machine's .stm source text (collapsible view), if known

    @property
    def timers(self) -> list[tuple[str, float]]:
        return self.pending.timers

    @property
    def inbound(self) -> list[OutboxEntry]:
        return self.pending.outbox

    @property
    def spawns(self) -> list[SpawnEntry]:
        return self.pending.spawns


class MonitorModel:
    def __init__(self, store: ExecutionStore, source: Optional[DefinitionSource] = None) -> None:
        self._store = store
        self._source = source or DefinitionSource.empty()
        self._published: dict[tuple[str, int], Any] = {}  # (id, version) -> its record, or None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        return self._store.list_executions(
            status=status, definition_id=definition_id, roots_only=roots_only, limit=limit, cursor=cursor
        )

    def _definition(self, exe: Execution) -> Optional[Definition]:
        """The Definition of `exe`'s version: the monitor's own, else the one published in the
        store (only shown and used to cancel — the monitor runs no actions)."""
        version = exe.definition_version or 1
        defn = self._source.get(exe.definition_id, exe.definition_fqn, version)
        if defn is None and exe.definition_fqn is None:
            built = self._published_version(exe.definition_id, version)
            defn = built[0] if built is not None else None
        return defn

    def _published_version(self, definition_id: str, version: int) -> Optional[tuple[Definition, str]]:
        """The published version, built, with its main file's text (None if not published)."""
        key = (definition_id, version)
        if key not in self._published:
            from harel.published import definition_from_record

            load = getattr(self._store, "load_definition", None)
            try:
                record = load(definition_id, version) if load is not None else None
                self._published[key] = (
                    (definition_from_record(record), record.source["files"][record.source["main"]])
                    if record is not None
                    else None
                )
            except Exception:  # never let a published version crash the UI: show data only
                self._published[key] = None
        return self._published[key]

    def detail(self, execution_id: str) -> Optional[ExecutionDetail]:
        """Snapshot one Execution: load it, resolve its Definition (None => data-only
        tree), build the highlight tree, and read what is waiting for it — by its id, so a
        refresh costs the same however large the store. Returns None if it no longer exists."""
        exe = self._store.load(execution_id)
        if exe is None:
            return None
        defn = self._definition(exe)
        # the timeline is the store's opt-in trace (HAREL_TRACE); it records `context_out` per
        # step, so each step's `context_in` is the previous one's (the first starts empty)
        trace = []
        prev_ctx: dict = {}
        for raw in self._store.read_trace(execution_id):
            step = TraceStep.from_dict({**raw, "context_in": raw.get("context_in") or prev_ctx})
            trace.append(step)
            prev_ctx = step.context_out
        source = self._source.source(exe.definition_id, exe.definition_version or 1)
        if source is None and exe.definition_fqn is None:
            built = self._published_version(exe.definition_id, exe.definition_version or 1)
            source = built[1] if built is not None else None
        children = [
            ChildRow(
                id=child_id,
                kind="invoke" if child.submachine else "region",
                root_path=child.root_path,
                finished=child.finished,
                outcome=child.outcome,
            )
            for child_id, child in exe.children.items()
        ]
        return ExecutionDetail(
            execution=exe,
            tree=build_tree_model(defn, exe),
            pending=self._store.pending_work(execution_id),
            children=children,
            trace=trace,
            source=source,
        )

    def get(self, execution_id: str) -> Optional[Execution]:
        return self._store.load(execution_id)

    def close(self) -> None:
        """Release the store's backend resources (connection/client)."""
        self._store.close()

    # --- control plane (the monitor's actions; each CASes the record at the next
    #     event boundary). suspend/resume/terminate need no Definition; cancel does. ---

    def suspend(self, execution_id: str) -> None:
        from harel.engine import control

        control.suspend(self._store, execution_id)

    def resume(self, execution_id: str) -> None:
        from harel.engine import control

        control.resume(self._store, execution_id)

    def terminate(self, execution_id: str) -> None:
        from harel.engine import control

        control.terminate(self._store, execution_id)

    def can_redrive(self, execution_id: str) -> bool:
        """Redrive needs the Definition: its target is a state of the machine."""
        exe = self._store.load(execution_id)
        return exe is not None and exe.status is Status.FAILED and self._definition(exe) is not None

    def redrive(self, execution_id: str, target_path: str) -> None:
        """Bring a dead-lettered execution back at `target_path`, a leaf state of its machine
        (`control.redrive`, which refuses a target it can't resume from with a reason)."""
        from harel.engine import control

        exe = self._store.load(execution_id)
        if exe is None:
            return
        defn = self._definition(exe)
        if defn is None:
            raise ValueError("redrive needs the machine's definition, which this monitor can't find")
        control.redrive(self._store, defn, execution_id, target_path)

    def can_cancel(self, execution_id: str) -> bool:
        """Cancel needs the Definition (to decide cooperative vs forceful). When the
        Definition can't be resolved, the UI disables cancel (terminate still works)."""
        exe = self._store.load(execution_id)
        return exe is not None and self._definition(exe) is not None

    def cancel(self, execution_id: str, reason: Optional[dict] = None) -> None:
        """Modelled cancel if the Definition resolves; otherwise a forceful terminate."""
        from harel.engine import control

        exe = self._store.load(execution_id)
        if exe is None:
            return
        defn = self._definition(exe)
        if defn is None:
            control.terminate(self._store, execution_id)
        else:
            control.cancel(self._store, defn, execution_id, reason=reason)
