"""Shared pieces of the persistence seam: the `ExecutionStore` Protocol, the
outbox/spawn/timer dataclasses, the `StoreConflict` error, and pagination helpers.

The concrete backends live in sibling modules (`sqlite.py`, `redis.py`, ...) and the
package `__init__` re-exports them, so `from harel.engine.store import RedisStore`
keeps working unchanged.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Iterable, Optional, Protocol, runtime_checkable

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.spec.states import Event

DEFAULT_TRACE_MAX = 200  # ring size: the store keeps only the last N trace steps per execution

# Fast-path Redis `commit` (shared by the sync + async backends) for the common case where the
# event only advances state — no emits, spawns, timers, or trace. Does the version-CAS and the
# write in ONE atomic round-trip (read version, compare to expected, SET + optional dedupe SADD)
# instead of the WATCH + GET + MULTI/EXEC dance (~3 round-trips). Atomicity replaces optimistic
# locking — the script either commits or returns a conflict, with no retry. Complex commits
# (anything to enqueue) still take the battle-tested WATCH/MULTI path. On conflict it returns an
# error reply `STM_CONFLICT:<current_version>` (raised as a ResponseError, mapped to StoreConflict).
# KEYS[1]=exe key, KEYS[2]=processed set; ARGV = exe_json (version pre-bumped), old_version,
# processed_event_id ('' if none).
_COMMIT_CAS_LUA = """
local cur = redis.call('GET', KEYS[1])
local curv = false
if cur then curv = cjson.decode(cur)['version'] end
local old = tonumber(ARGV[2])
if not (cur == false and old == 0) and curv ~= old then
  return redis.error_reply('STM_CONFLICT:' .. tostring(curv))
end
redis.call('SET', KEYS[1], ARGV[1])
if ARGV[3] ~= '' then
  redis.call('SADD', KEYS[2], ARGV[3])
end
return 1
"""


def _encode_offset(offset: int) -> str:
    """An opaque pagination cursor over an integer offset (the SQL/Mongo/Dict backends)."""
    return base64.urlsafe_b64encode(str(offset).encode()).decode()


def _decode_offset(cursor: Optional[str]) -> int:
    """Decode an offset cursor; a missing/garbage cursor means start from 0."""
    if not cursor:
        return 0
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except (ValueError, TypeError):
        return 0


def _matches(summary: ExecutionSummary, status, definition_id, roots_only) -> bool:
    """Client-side filter shared by the backends that can't filter inside the JSON blob."""
    if status is not None and summary.status not in status:
        return False
    if definition_id is not None and summary.definition_id != definition_id:
        return False
    if roots_only and summary.parent_id is not None:
        return False
    return True


@dataclass
class OutboxEntry:
    """A deferred event awaiting delivery: `seq` (monotonic, for ack), the
    `target_id` Execution to deliver to (None = no target), and the `event`."""

    seq: int
    target_id: Optional[str]
    event: Event


@dataclass
class SpawnEntry:
    """A pending child-Execution creation (an orthogonal fork), committed in the
    SAME transaction as the parent's advance + join expectations (`children` dict),
    so the fork is atomic and crash-safe. A relay creates the child afterwards,
    idempotently (skip if it already exists). Mirrors `OutboxEntry` for events."""

    seq: int
    parent_id: str
    child_id: str
    root_path: str
    context: dict


@dataclass
class TimerOp:
    """A durable-timer mutation applied atomically with a `commit`: `schedule`
    arms (upserts) the timer for `(execution_id, path)` to fire at `fire_at`;
    `cancel` disarms it. Keyed by `(execution_id, path)` — re-entry replaces."""

    action: str  # "schedule" | "cancel"
    path: str
    fire_at: float = 0.0


class StoreConflict(RuntimeError):
    """Raised when a save loses the optimistic-concurrency check: the stored row
    moved past the version the Execution was loaded at (another writer won). The
    caller should reload and retry, or drop the stale work."""

    def __init__(self, execution_id: str, expected: int, found: Optional[int]) -> None:
        super().__init__(f"stale write to {execution_id}: expected version {expected}, found {found}")
        self.execution_id = execution_id
        self.expected = expected
        self.found = found


@runtime_checkable
class ExecutionStore(Protocol):
    def load(self, execution_id: str) -> Optional[Execution]: ...

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        """A page of lightweight `ExecutionSummary` for a monitor/list view. `status`
        matches any of the given statuses (OR); `definition_id` is an exact match;
        `roots_only` keeps only `parent_id is None`. `cursor` is the opaque
        `next_cursor` from a prior page (None = first page); paginate until the
        returned `next_cursor` is None.

        Ordering is stable by `id` on the in-memory/SQL/document backends, but
        **best-effort and unordered** on Redis (SCAN) and DynamoDB (Scan) — a caller
        that needs an order sorts client-side. `status`/`outcome`/`parent_id` are not
        broken-out columns in the durable backends (they live inside the JSON blob),
        so on some backends those filters run client-side, which means a page may
        return fewer than `limit` matches — keep paging while `next_cursor` is set."""
        ...

    def save(self, exe: Execution) -> None:
        """Persist `exe`, committing `version+1` iff the stored row is still at
        `exe.version` (optimistic concurrency); raise `StoreConflict` otherwise.
        On success `exe.version` is bumped to the committed value."""
        ...

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: "tuple[TimerOp, ...]" = (),
        spawns: "tuple[tuple[str, str, dict], ...]" = (),
        trace: "Optional[dict]" = None,
    ) -> None:
        """Atomically `save` the Execution, enqueue its emitted events into the
        outbox, record `processed_event_id` as handled (if given), apply the
        `timers` mutations, and enqueue the `spawns` (orthogonal child creations,
        each `(child_id, root_path, context)`). Either all happen or none — so a
        fork's children + the parent's join expectations commit atomically.

        `trace` (opt-in execution-trace, off by default): one step to append to the
        execution's timeline (event/transition/actions/context_out + a stamped `index`),
        written in the SAME transaction as the advance — no extra round-trip or fsync,
        and `load` is unaffected (it still reads the snapshot, not a replay). The store
        keeps only the last `trace_max` steps (a ring). Recorded by the SQL-family and
        Dict backends; the others accept and ignore it for now."""
        ...

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        """Whether `execution_id` already processed `event_id` (dedupe under
        at-least-once delivery)."""
        ...

    def pending_outbox(self) -> list[OutboxEntry]:
        """Undelivered outbox entries, oldest first."""
        ...

    def ack_outbox(self, seq: int) -> None:
        """Mark the outbox entry `seq` delivered (remove it)."""
        ...

    def pending_spawns(self) -> "list[SpawnEntry]":
        """Undelivered child-creation intents, oldest first."""
        ...

    def ack_spawn(self, seq: int) -> None:
        """Mark the spawn entry `seq` done (remove it)."""
        ...

    def due_timers(self, now: float) -> "list[tuple[str, str, float]]":
        """Timers due at `now` (fire_at <= now), as `(execution_id, path, fire_at)`."""
        ...

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        """Remove the timer for `(execution_id, path)` — but only if it still holds
        `fire_at` (so a concurrent re-schedule to a new time survives a stale sweep)."""
        ...

    def close(self) -> None:
        """Release any backend resources (connection/client). No-op for in-memory."""
        ...
