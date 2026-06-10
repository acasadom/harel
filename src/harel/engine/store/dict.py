"""DictStore — a durable ExecutionStore backend."""

from __future__ import annotations

from typing import Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.store._base import (
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    _decode_offset,
    _encode_offset,
    _matches,
)
from harel.spec.states import Event


class DictStore:
    """In-memory `ExecutionStore`: a plain dict. Returns the same `Execution`
    object that was saved (no serialization), so callers that hold a reference
    see mutations — the default for embedded, non-durable runs."""

    def __init__(self) -> None:
        self._by_id: dict[str, Execution] = {}
        self._outbox: list[OutboxEntry] = []
        self._processed: set[tuple[str, str]] = set()
        self._timers: dict[tuple[str, str], float] = {}  # (execution_id, path) -> fire_at
        self._spawns: list[SpawnEntry] = []
        self._trace: dict[str, list[dict]] = {}  # execution_id -> ordered trace steps (preview)
        self._seq = 0
        self._spawn_seq = 0

    # --- execution trace (PREVIEW seam, NOT on the Protocol yet) ---------------------
    # The engine does not record a step-by-step trace today; this read/append pair lets
    # the monitor's timeline render (seeded) data while that engine feature is designed.

    def append_trace(self, execution_id: str, entry: dict) -> None:
        steps = self._trace.setdefault(execution_id, [])
        steps.append({**entry, "index": entry.get("index", len(steps))})

    def read_trace(self, execution_id: str) -> list[dict]:
        return list(self._trace.get(execution_id, []))

    def load(self, execution_id: str) -> Optional[Execution]:
        return self._by_id.get(execution_id)

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        status = set(status) if status is not None else None
        summaries = [ExecutionSummary.of(e) for e in self._by_id.values()]
        summaries = [s for s in summaries if _matches(s, status, definition_id, roots_only)]
        summaries.sort(key=lambda s: s.id)  # stable order for deterministic pagination
        off = _decode_offset(cursor)
        window = summaries[off : off + limit]
        nxt = _encode_offset(off + limit) if off + limit < len(summaries) else None
        return ExecutionPage(items=window, next_cursor=nxt)

    def save(self, exe: Execution) -> None:
        prev = self._by_id.get(exe.id)
        # CAS only bites when a *different* object is stored under the same id
        # (a genuine concurrent writer); the common same-object case always wins.
        if prev is not None and prev is not exe and prev.version != exe.version:
            raise StoreConflict(exe.id, expected=exe.version, found=prev.version)
        exe.version += 1
        self._by_id[exe.id] = exe

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        self.save(exe)  # CAS first: raises before any emit is enqueued
        for target_id, event in emits:
            self._seq += 1
            self._outbox.append(OutboxEntry(self._seq, target_id, event))
        if processed_event_id is not None:
            self._processed.add((exe.id, processed_event_id))
        for op in timers:
            if op.action == "schedule":
                self._timers[(exe.id, op.path)] = op.fire_at
            else:
                self._timers.pop((exe.id, op.path), None)
        for child_id, root_path, context in spawns:
            self._spawn_seq += 1
            self._spawns.append(SpawnEntry(self._spawn_seq, exe.id, child_id, root_path, dict(context)))

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        return (execution_id, event_id) in self._processed

    def pending_outbox(self) -> list[OutboxEntry]:
        return list(self._outbox)

    def ack_outbox(self, seq: int) -> None:
        self._outbox = [e for e in self._outbox if e.seq != seq]

    def pending_spawns(self) -> list[SpawnEntry]:
        return list(self._spawns)

    def ack_spawn(self, seq: int) -> None:
        self._spawns = [s for s in self._spawns if s.seq != seq]

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        return [(eid, path, fa) for (eid, path), fa in self._timers.items() if fa <= now]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        if self._timers.get((execution_id, path)) == fire_at:
            del self._timers[(execution_id, path)]

    def close(self) -> None:
        pass  # nothing to release; the dict lives with the process
