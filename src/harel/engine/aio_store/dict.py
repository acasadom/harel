"""AsyncDictStore — an async ExecutionStore backend."""

from __future__ import annotations

from typing import Optional

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.spec.states import Event


class AsyncDictStore:
    """In-memory `AsyncExecutionStore`: the async mirror of `DictStore`. Returns the same
    `Execution` object that was saved (no serialization), so callers holding a reference see
    mutations — the identity contract the in-place test harness relies on. No lock: a single
    event loop schedules cooperatively and none of these methods await internally, so each
    runs atomically between suspension points."""

    def __init__(self) -> None:
        self._by_id: dict[str, Execution] = {}
        self._outbox: list[OutboxEntry] = []
        self._processed: set[tuple[str, str]] = set()
        self._timers: dict[tuple[str, str], float] = {}
        self._spawns: list[SpawnEntry] = []
        self._seq = 0
        self._spawn_seq = 0

    async def load(self, execution_id: str) -> Optional[Execution]:
        return self._by_id.get(execution_id)

    async def save(self, exe: Execution) -> None:
        prev = self._by_id.get(exe.id)
        if prev is not None and prev is not exe and prev.version != exe.version:
            raise StoreConflict(exe.id, expected=exe.version, found=prev.version)
        exe.version += 1
        self._by_id[exe.id] = exe

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        await self.save(exe)  # CAS first: raises before any emit is enqueued
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

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        return (execution_id, event_id) in self._processed

    async def pending_outbox(self) -> list[OutboxEntry]:
        return list(self._outbox)

    async def ack_outbox(self, seq: int) -> None:
        self._outbox = [e for e in self._outbox if e.seq != seq]

    async def pending_spawns(self) -> list[SpawnEntry]:
        return list(self._spawns)

    async def ack_spawn(self, seq: int) -> None:
        self._spawns = [s for s in self._spawns if s.seq != seq]

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        return [(eid, path, fa) for (eid, path), fa in self._timers.items() if fa <= now]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        if self._timers.get((execution_id, path)) == fire_at:
            del self._timers[(execution_id, path)]

    async def close(self) -> None:
        pass
