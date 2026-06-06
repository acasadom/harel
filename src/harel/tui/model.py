"""The monitor's read model — a pure facade over an `ExecutionStore` plus a
`DefinitionSource`. No textual, no async, no timers of its own: it snapshots the store
and builds the renderable data the UI displays. The textual app calls these from worker
threads so a slow store never blocks the UI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, Status
from harel.engine.store import ExecutionStore, OutboxEntry, SpawnEntry
from harel.tui.resolve import DefinitionSource
from harel.tui.tree import TreeModel, build_tree_model

# a horizon far beyond any real fire_at: `due_timers(now)` returns fire_at <= now, so a
# huge `now` yields ALL of an execution's timers (there is no "all timers" store method).
_ALL_TIMERS_HORIZON = 1e18


@dataclass
class ExecutionDetail:
    """Everything the detail screen shows for one Execution: the full record, the
    statechart highlight tree, and the pending work scoped to this id."""

    execution: Execution
    tree: TreeModel
    timers: list[tuple[str, float]] = field(default_factory=list)  # (path, fire_at)
    inbound: list[OutboxEntry] = field(default_factory=list)  # outbox entries targeting this id
    spawns: list[SpawnEntry] = field(default_factory=list)  # pending child creations from this id


class MonitorModel:
    def __init__(self, store: ExecutionStore, source: Optional[DefinitionSource] = None) -> None:
        self._store = store
        self._source = source or DefinitionSource.empty()

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

    def detail(self, execution_id: str) -> Optional[ExecutionDetail]:
        """Snapshot one Execution: load it, resolve its Definition (None => data-only
        tree), build the highlight tree, and collect its pending timers/outbox/spawns.
        Returns None if the execution no longer exists."""
        exe = self._store.load(execution_id)
        if exe is None:
            return None
        defn = self._source.get(exe.definition_id, exe.definition_fqn)
        tree = build_tree_model(defn, exe)
        timers = [
            (path, fire_at)
            for (eid, path, fire_at) in self._store.due_timers(_ALL_TIMERS_HORIZON)
            if eid == execution_id
        ]
        timers.sort(key=lambda t: t[1])
        inbound = [e for e in self._store.pending_outbox() if e.target_id == execution_id]
        spawns = [s for s in self._store.pending_spawns() if s.parent_id == execution_id]
        return ExecutionDetail(execution=exe, tree=tree, timers=timers, inbound=inbound, spawns=spawns)

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

    def can_cancel(self, execution_id: str) -> bool:
        """Cancel needs the Definition (to decide cooperative vs forceful). When the
        Definition can't be resolved, the UI disables cancel (terminate still works)."""
        exe = self._store.load(execution_id)
        return exe is not None and self._source.get(exe.definition_id, exe.definition_fqn) is not None

    def cancel(self, execution_id: str, reason: Optional[dict] = None) -> None:
        """Modelled cancel if the Definition resolves; otherwise a forceful terminate."""
        from harel.engine import control

        exe = self._store.load(execution_id)
        if exe is None:
            return
        defn = self._source.get(exe.definition_id, exe.definition_fqn)
        if defn is None:
            control.terminate(self._store, execution_id)
        else:
            control.cancel(self._store, defn, execution_id, reason=reason)
