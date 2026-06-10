"""The persistence seam for Executions.

An `ExecutionStore` is how the `Driver` reads and checkpoints the running
instances, plus a **transactional outbox** for the deferred events an Execution
emits (e.g. a region's `Finished` to its parent). `commit` writes the Execution
and its emitted events in the *same* transaction, so a crash can never leave the
state advanced but the `Finished` unsent (which would deadlock the join). A
separate relay reads the outbox and delivers it after the commit.

The in-memory `DictStore` (default) keeps everything in dicts/lists — same
object identity in and out, so it is behaviour-preserving. A durable backend
(SQL/DBOS/SQS/...) implements the same methods over the JSON-serializable
`Execution`; the `Driver` then checkpoints at each event boundary and the same
engine drives durable runs.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Optional, Protocol, Union, runtime_checkable

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.spec.states import Event


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
    ) -> None:
        """Atomically `save` the Execution, enqueue its emitted events into the
        outbox, record `processed_event_id` as handled (if given), apply the
        `timers` mutations, and enqueue the `spawns` (orthogonal child creations,
        each `(child_id, root_path, context)`). Either all happen or none — so a
        fork's children + the parent's join expectations commit atomically."""
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


class SqliteStore:
    """A durable `ExecutionStore` over SQLite (stdlib): each Execution is stored
    as JSON keyed by id, committed on every save. A fresh `SqliteStore` on the
    same file reads the committed state — so a run survives a process restart and
    resumes. `:memory:` gives a non-persistent variant for tests."""

    def __init__(self, path: Union[str, Path] = ":memory:") -> None:
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")  # readers don't block the single writer
        self._conn.execute("PRAGMA busy_timeout=5000")  # wait for the write-lock instead of erroring
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS executions "
            "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, "
            "version INTEGER NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS outbox "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS processed_events "
            "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, event_id))"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS timers "
            "(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at REAL NOT NULL, "
            "PRIMARY KEY (execution_id, path))"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS spawns "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
            "root_path TEXT NOT NULL, context TEXT NOT NULL)"
        )
        # PREVIEW: execution trace for the monitor timeline (not yet engine-written)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS trace "
            "(execution_id TEXT NOT NULL, idx INTEGER NOT NULL, entry TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, idx))"
        )
        self._conn.commit()

    def append_trace(self, execution_id: str, entry: dict) -> None:
        """PREVIEW seam (see DictStore): append a trace step for the monitor timeline."""
        (count,) = self._conn.execute(
            "SELECT COUNT(*) FROM trace WHERE execution_id = ?", (execution_id,)
        ).fetchone()
        idx = entry.get("index", count)
        self._conn.execute(
            "INSERT OR REPLACE INTO trace (execution_id, idx, entry) VALUES (?, ?, ?)",
            (execution_id, idx, json.dumps({**entry, "index": idx})),
        )
        self._conn.commit()

    def read_trace(self, execution_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT entry FROM trace WHERE execution_id = ? ORDER BY idx", (execution_id,)
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def load(self, execution_id: str) -> Optional[Execution]:
        row = self._conn.execute("SELECT data FROM executions WHERE id = ?", (execution_id,)).fetchone()
        return Execution.model_validate_json(row[0]) if row is not None else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # project only the scalar summary fields out of the JSON blob (never pull `data`);
        # status/outcome/active_path/parent_id live inside it, so json_extract reaches them.
        where, params = ["1=1"], []
        if definition_id is not None:
            where.append("definition_id = ?")
            params.append(definition_id)
        if status is not None:
            statuses = [s.value for s in status]
            where.append(f"json_extract(data,'$.status') IN ({','.join('?' * len(statuses))})")
            params += statuses
        if roots_only:
            where.append("json_extract(data,'$.parent_id') IS NULL")
        off = _decode_offset(cursor)
        rows = self._conn.execute(
            "SELECT id, definition_id, version, json_extract(data,'$.status'), "
            "json_extract(data,'$.outcome'), json_extract(data,'$.active_path'), "
            "json_extract(data,'$.parent_id') FROM executions "
            f"WHERE {' AND '.join(where)} ORDER BY id LIMIT ? OFFSET ?",
            (*params, limit + 1, off),  # fetch one extra to know if there's a next page
        ).fetchall()
        items = [
            ExecutionSummary(
                id=r[0],
                definition_id=r[1],
                version=r[2],
                status=r[3],
                outcome=r[4],
                active_path=r[5],
                parent_id=r[6],
            )
            for r in rows[:limit]
        ]
        nxt = _encode_offset(off + limit) if len(rows) > limit else None
        return ExecutionPage(items=items, next_cursor=nxt)

    def _write(self, exe: Execution) -> None:
        """The CAS write of `exe`, WITHOUT committing the transaction (so it can
        be batched atomically with outbox inserts in `commit`)."""
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        cur = self._conn.execute(
            "UPDATE executions SET data = ?, version = ? WHERE id = ? AND version = ?",
            (data, exe.version, exe.id, old),
        )
        if cur.rowcount == 0:
            # no row matched `old`: either a brand-new Execution (no row yet) or a
            # stale write (the row moved past `old`). Distinguish by existence.
            found = self._conn.execute("SELECT version FROM executions WHERE id = ?", (exe.id,)).fetchone()
            if found is None and old == 0:
                self._conn.execute(
                    "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
                    (exe.id, exe.definition_id, data, exe.version),
                )
            else:
                exe.version = old  # undo the in-memory bump; the commit did not happen
                raise StoreConflict(exe.id, expected=old, found=found[0] if found else None)

    def save(self, exe: Execution) -> None:
        try:
            self._write(exe)
            self._conn.commit()
        except StoreConflict:
            self._conn.rollback()
            raise

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        try:
            self._write(exe)
            for target_id, event in emits:
                self._conn.execute(
                    "INSERT INTO outbox (target_id, event) VALUES (?, ?)",
                    (target_id, event.model_dump_json()),
                )
            if processed_event_id is not None:
                self._conn.execute(
                    "INSERT OR IGNORE INTO processed_events (execution_id, event_id) VALUES (?, ?)",
                    (exe.id, processed_event_id),
                )
            for child_id, root_path, context in spawns:
                self._conn.execute(
                    "INSERT INTO spawns (parent_id, child_id, root_path, context) VALUES (?, ?, ?, ?)",
                    (exe.id, child_id, root_path, json.dumps(context)),
                )
            for op in timers:
                if op.action == "schedule":
                    self._conn.execute(
                        "INSERT INTO timers (execution_id, path, fire_at) VALUES (?, ?, ?) "
                        "ON CONFLICT(execution_id, path) DO UPDATE SET fire_at = excluded.fire_at",
                        (exe.id, op.path, op.fire_at),
                    )
                else:
                    self._conn.execute(
                        "DELETE FROM timers WHERE execution_id = ? AND path = ?", (exe.id, op.path)
                    )
            self._conn.commit()
        except StoreConflict:
            self._conn.rollback()
            raise

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        ).fetchone()
        return row is not None

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._conn.execute(
            "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq"
        ).fetchall()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        self._conn.execute("DELETE FROM spawns WHERE seq = ?", (seq,))
        self._conn.commit()

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._conn.execute(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
        ).fetchall()
        return [(eid, path, fa) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._conn.execute(
            "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
            (execution_id, path, fire_at),
        )
        self._conn.commit()

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._conn.execute("SELECT seq, target_id, event FROM outbox ORDER BY seq").fetchall()
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._conn.execute("DELETE FROM outbox WHERE seq = ?", (seq,))
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class LibsqlStore:
    """Durable `ExecutionStore` over **libSQL** (Turso's SQLite fork) via the `libsql`
    package — SQLite-compatible (DB-API), so the SQL, the version-CAS and the one-transaction
    `commit` are identical to `SqliteStore`.

    **EXPERIMENTAL**: the local-file path is covered in-process by the test suite; the Turso/
    `sqld` embedded-replica path (``sync_url``) is wired but not yet validated against a real
    Turso account, and its primary-follower replication is eventually consistent (read from the
    primary for CAS, or expect extra `StoreConflict` retries). The connection adapts by argument:

    - a local file (``LibsqlStore("state.db")``) — like SQLite;
    - an **embedded replica** (``sync_url=`` + ``auth_token=``) — local reads from the file,
      writes routed to the Turso/`sqld` primary and synced back;
    - so the same backend is a single-file embed AND a distributed (Turso/`sqld`) store.

    `libsql` is synchronous (a `sqlite3` driver); the async worker reaches it through
    `AsyncLibsqlStore`, which off-loads to a thread. `:memory:` is the test variant."""

    def __init__(
        self,
        database: Union[str, Path] = ":memory:",
        *,
        auth_token: str = "",
        sync_url: Optional[str] = None,
        sync_interval: Optional[float] = None,
    ) -> None:
        import libsql

        kwargs: dict[str, Any] = {"_check_same_thread": False}
        if sync_url is not None:  # embedded replica against a Turso/sqld primary
            kwargs["sync_url"] = sync_url
            kwargs["auth_token"] = auth_token
            if sync_interval is not None:
                kwargs["sync_interval"] = sync_interval
        self._conn = libsql.connect(str(database), **kwargs)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS executions "
            "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INTEGER NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS outbox "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS processed_events "
            "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS timers "
            "(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at REAL NOT NULL, "
            "PRIMARY KEY (execution_id, path))"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS spawns "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
            "root_path TEXT NOT NULL, context TEXT NOT NULL)"
        )
        self._conn.commit()

    def load(self, execution_id: str) -> Optional[Execution]:
        row = self._conn.execute("SELECT data FROM executions WHERE id = ?", (execution_id,)).fetchone()
        return Execution.model_validate_json(row[0]) if row is not None else None

    def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one query (the worker's per-event pair)."""
        row = self._conn.execute(
            "SELECT (SELECT data FROM executions WHERE id = ?), "
            "EXISTS(SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?)",
            (execution_id, execution_id, event_id),
        ).fetchone()
        if row is None or row[0] is None:
            return None, False
        return Execution.model_validate_json(row[0]), bool(row[1])

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        where, params = ["1=1"], []
        if definition_id is not None:
            where.append("definition_id = ?")
            params.append(definition_id)
        if status is not None:
            statuses = [s.value for s in status]
            where.append(f"json_extract(data,'$.status') IN ({','.join('?' * len(statuses))})")
            params += statuses
        if roots_only:
            where.append("json_extract(data,'$.parent_id') IS NULL")
        off = _decode_offset(cursor)
        rows = self._conn.execute(
            "SELECT id, definition_id, version, json_extract(data,'$.status'), "
            "json_extract(data,'$.outcome'), json_extract(data,'$.active_path'), "
            "json_extract(data,'$.parent_id') FROM executions "
            f"WHERE {' AND '.join(where)} ORDER BY id LIMIT ? OFFSET ?",
            (*params, limit + 1, off),
        ).fetchall()
        items = [
            ExecutionSummary(
                id=r[0],
                definition_id=r[1],
                version=r[2],
                status=r[3],
                outcome=r[4],
                active_path=r[5],
                parent_id=r[6],
            )
            for r in rows[:limit]
        ]
        nxt = _encode_offset(off + limit) if len(rows) > limit else None
        return ExecutionPage(items=items, next_cursor=nxt)

    def _write(self, exe: Execution) -> None:
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        cur = self._conn.execute(
            "UPDATE executions SET data = ?, version = ? WHERE id = ? AND version = ?",
            (data, exe.version, exe.id, old),
        )
        if cur.rowcount == 0:
            found = self._conn.execute("SELECT version FROM executions WHERE id = ?", (exe.id,)).fetchone()
            if found is None and old == 0:
                self._conn.execute(
                    "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
                    (exe.id, exe.definition_id, data, exe.version),
                )
            else:
                exe.version = old
                raise StoreConflict(exe.id, expected=old, found=found[0] if found else None)

    def save(self, exe: Execution) -> None:
        try:
            self._write(exe)
            self._conn.commit()
        except StoreConflict:
            self._conn.rollback()
            raise

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        try:
            self._write(exe)
            for target_id, event in emits:
                self._conn.execute(
                    "INSERT INTO outbox (target_id, event) VALUES (?, ?)",
                    (target_id, event.model_dump_json()),
                )
            if processed_event_id is not None:
                self._conn.execute(
                    "INSERT OR IGNORE INTO processed_events (execution_id, event_id) VALUES (?, ?)",
                    (exe.id, processed_event_id),
                )
            for child_id, root_path, context in spawns:
                self._conn.execute(
                    "INSERT INTO spawns (parent_id, child_id, root_path, context) VALUES (?, ?, ?, ?)",
                    (exe.id, child_id, root_path, json.dumps(context)),
                )
            for op in timers:
                if op.action == "schedule":
                    self._conn.execute(
                        "INSERT INTO timers (execution_id, path, fire_at) VALUES (?, ?, ?) "
                        "ON CONFLICT(execution_id, path) DO UPDATE SET fire_at = excluded.fire_at",
                        (exe.id, op.path, op.fire_at),
                    )
                else:
                    self._conn.execute(
                        "DELETE FROM timers WHERE execution_id = ? AND path = ?", (exe.id, op.path)
                    )
            self._conn.commit()
        except StoreConflict:
            self._conn.rollback()
            raise

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        ).fetchone()
        return row is not None

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._conn.execute("SELECT seq, target_id, event FROM outbox ORDER BY seq").fetchall()
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._conn.execute("DELETE FROM outbox WHERE seq = ?", (seq,))
        self._conn.commit()

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._conn.execute(
            "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq"
        ).fetchall()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        self._conn.execute("DELETE FROM spawns WHERE seq = ?", (seq,))
        self._conn.commit()

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._conn.execute(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
        ).fetchall()
        return [(eid, path, fa) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._conn.execute(
            "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
            (execution_id, path, fire_at),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class RedisStore:
    """A durable `ExecutionStore` over Redis — the all-network alternative to
    `SqliteStore` (no shared filesystem, so it works across machines / containers
    without a shared volume). Pairs naturally with `RedisTransport` for a pure-
    Redis deployment. The client is injected (duck-typed), so `redis` stays an
    optional dependency and tests use fakeredis.

    Keys (under `prefix`): ``exe:{id}`` = the Execution JSON; ``outbox`` = a hash
    {seq -> emit}; ``outbox:seq`` = the monotonic counter; ``processed:{id}`` = a
    set of handled event ids. `commit` is atomic via WATCH/MULTI/EXEC on the
    Execution key (no Lua, so fakeredis supports it): the version CAS is checked
    under WATCH and the writes go in one EXEC; a concurrent change → `StoreConflict`."""

    def __init__(self, client: Any, prefix: str = "stm") -> None:
        from redis.exceptions import WatchError

        self._r = client
        self._prefix = prefix
        self._WatchError = WatchError

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "RedisStore":
        """Convenience constructor; imports `redis` lazily (the optional dep)."""
        import redis

        return cls(redis.Redis.from_url(url), prefix)

    def _k(self, suffix: str) -> str:
        return f"{self._prefix}:{suffix}"

    def load(self, execution_id: str) -> Optional[Execution]:
        raw = self._r.get(self._k(f"exe:{execution_id}"))
        return Execution.model_validate_json(raw) if raw is not None else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # Redis can't query inside a value, so list = SCAN the exe:* keys + MGET +
        # filter client-side. Order is arbitrary and a page is best-effort sized (one
        # SCAN round); keep paging while next_cursor is set. cursor = native SCAN cursor.
        status = set(status) if status is not None else None
        cur = int(cursor) if cursor else 0
        new_cur, keys = self._r.scan(cursor=cur, match=self._k("exe:*"), count=max(limit, 20))
        items = []
        for raw in self._r.mget(keys) if keys else []:
            if not raw:
                continue
            data = json.loads(raw)
            summary = ExecutionSummary.from_data(data, data.get("version", 0))
            if _matches(summary, status, definition_id, roots_only):
                items.append(summary)
        items.sort(key=lambda s: s.id)  # within-page order only (no global order in SCAN)
        return ExecutionPage(items=items, next_cursor=str(new_cur) if new_cur != 0 else None)

    def save(self, exe: Execution) -> None:
        self.commit(exe, [])

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        # allocate monotonic outbox seqs up front (INCR can't return its value
        # inside MULTI; a seq wasted by an aborted txn is harmless)
        queued = [(int(self._r.incr(self._k("outbox:seq"))), t, e.model_dump_json()) for t, e in emits]
        queued_spawns = [(int(self._r.incr(self._k("spawns:seq"))), cid, rp, ctx) for cid, rp, ctx in spawns]
        key = self._k(f"exe:{exe.id}")
        old = exe.version
        with self._r.pipeline() as pipe:
            try:
                pipe.watch(key)
                current = pipe.get(key)
                cur_version = json.loads(current)["version"] if current is not None else None
                if not (current is None and old == 0) and cur_version != old:
                    pipe.unwatch()
                    raise StoreConflict(exe.id, expected=old, found=cur_version)
                exe.version = old + 1
                pipe.multi()
                pipe.set(key, exe.model_dump_json())
                for seq, target_id, event_json in queued:
                    pipe.hset(self._k("outbox"), str(seq), json.dumps({"t": target_id, "e": event_json}))
                if processed_event_id is not None:
                    pipe.sadd(self._k(f"processed:{exe.id}"), processed_event_id)
                for seq, cid, rp, ctx in queued_spawns:
                    pipe.hset(
                        self._k("spawns"),
                        str(seq),
                        json.dumps({"p": exe.id, "c": cid, "r": rp, "x": ctx}),
                    )
                for op in timers:
                    member = f"{exe.id}\x00{op.path}"
                    if op.action == "schedule":
                        pipe.zadd(self._k("timers"), {member: op.fire_at})
                    else:
                        pipe.zrem(self._k("timers"), member)
                pipe.execute()
            except self._WatchError:
                exe.version = old  # a concurrent writer won between WATCH and EXEC
                raise StoreConflict(exe.id, expected=old, found=None)

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        return bool(self._r.sismember(self._k(f"processed:{execution_id}"), event_id))

    def pending_spawns(self) -> list[SpawnEntry]:
        entries = []
        for seq_raw, val_raw in self._r.hgetall(self._k("spawns")).items():
            p = json.loads(val_raw)
            entries.append(SpawnEntry(int(seq_raw), p["p"], p["c"], p["r"], p["x"]))
        return sorted(entries, key=lambda s: s.seq)

    def ack_spawn(self, seq: int) -> None:
        self._r.hdel(self._k("spawns"), str(seq))

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        out: list[tuple[str, str, float]] = []
        for member_raw, score in self._r.zrangebyscore(self._k("timers"), "-inf", now, withscores=True):
            member = member_raw.decode() if isinstance(member_raw, (bytes, bytearray)) else member_raw
            execution_id, _, path = member.partition("\x00")
            out.append((execution_id, path, float(score)))
        return out

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        member = f"{execution_id}\x00{path}"
        score = self._r.zscore(self._k("timers"), member)
        if score is not None and float(score) == fire_at:
            self._r.zrem(self._k("timers"), member)

    def pending_outbox(self) -> list[OutboxEntry]:
        entries = []
        for seq_raw, val_raw in self._r.hgetall(self._k("outbox")).items():
            payload = json.loads(val_raw)
            entries.append(OutboxEntry(int(seq_raw), payload["t"], Event.model_validate_json(payload["e"])))
        return sorted(entries, key=lambda e: e.seq)

    def ack_outbox(self, seq: int) -> None:
        self._r.hdel(self._k("outbox"), str(seq))

    def close(self) -> None:
        self._r.close()


class PostgresStore:
    """A durable `ExecutionStore` over PostgreSQL (psycopg) — a real SQL server
    for the distributed-SQL deployment (state shared across machines without a
    filesystem). Same contract as SqliteStore: version/CAS, transactional outbox,
    dedupe; the whole `commit` is one Postgres transaction.

    The connection is injected (duck-typed) so `psycopg` is an optional extra. CAS
    is a plain `UPDATE ... WHERE version = old`: Postgres row-locks serialize
    concurrent writers, so exactly one wins (rowcount 1) and the loser (rowcount 0)
    raises `StoreConflict` — no app-level locking needed."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS executions "
                "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INT NOT NULL)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS outbox "
                "(seq BIGSERIAL PRIMARY KEY, target_id TEXT, event TEXT NOT NULL)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS processed_events "
                "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS timers "
                "(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at DOUBLE PRECISION NOT NULL, "
                "PRIMARY KEY (execution_id, path))"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS spawns "
                "(seq BIGSERIAL PRIMARY KEY, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
                "root_path TEXT NOT NULL, context TEXT NOT NULL)"
            )
        conn.commit()

    @classmethod
    def from_dsn(cls, dsn: str, connect_retries: int = 15, retry_delay: float = 1.0) -> "PostgresStore":
        """Convenience constructor; imports `psycopg` lazily (the optional dep).
        Retries the connection so a worker starting alongside Postgres (compose)
        waits for it to accept connections rather than crashing."""
        import time

        import psycopg

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(psycopg.connect(dsn))
            except psycopg.OperationalError as exc:
                last = exc
                time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("postgres connect failed")

    def load(self, execution_id: str) -> Optional[Execution]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT data FROM executions WHERE id = %s", (execution_id,))
            row = cur.fetchone()
        self._conn.commit()  # end the read transaction so the next read sees fresh data
        return Execution.model_validate_json(row[0]) if row is not None else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # `data` is TEXT, so cast to jsonb and extract the scalar summary fields with ->>
        # (never select the heavy blob). status filtered with = ANY(array).
        where: list[str] = ["TRUE"]
        params: list[Any] = []
        if definition_id is not None:
            where.append("definition_id = %s")
            params.append(definition_id)
        if status is not None:
            where.append("(data::jsonb->>'status') = ANY(%s)")
            params.append([s.value for s in status])
        if roots_only:
            where.append("(data::jsonb->>'parent_id') IS NULL")
        off = _decode_offset(cursor)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT id, definition_id, version, data::jsonb->>'status', "
                "data::jsonb->>'outcome', data::jsonb->>'active_path', data::jsonb->>'parent_id' "
                f"FROM executions WHERE {' AND '.join(where)} ORDER BY id LIMIT %s OFFSET %s",
                (*params, limit + 1, off),
            )
            rows = cur.fetchall()
        self._conn.commit()  # end the read transaction
        items = [
            ExecutionSummary(
                id=r[0],
                definition_id=r[1],
                version=r[2],
                status=r[3],
                outcome=r[4],
                active_path=r[5],
                parent_id=r[6],
            )
            for r in rows[:limit]
        ]
        nxt = _encode_offset(off + limit) if len(rows) > limit else None
        return ExecutionPage(items=items, next_cursor=nxt)

    def save(self, exe: Execution) -> None:
        self.commit(exe, [])

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "UPDATE executions SET data = %s, version = %s WHERE id = %s AND version = %s",
                    (data, exe.version, exe.id, old),
                )
                if cur.rowcount == 0:
                    cur.execute("SELECT version FROM executions WHERE id = %s", (exe.id,))
                    row = cur.fetchone()
                    if row is None and old == 0:
                        cur.execute(
                            "INSERT INTO executions (id, definition_id, data, version) VALUES (%s, %s, %s, %s)",
                            (exe.id, exe.definition_id, data, exe.version),
                        )
                    else:
                        exe.version = old
                        self._conn.rollback()
                        raise StoreConflict(exe.id, expected=old, found=row[0] if row else None)
                for target_id, event in emits:
                    cur.execute(
                        "INSERT INTO outbox (target_id, event) VALUES (%s, %s)",
                        (target_id, event.model_dump_json()),
                    )
                if processed_event_id is not None:
                    cur.execute(
                        "INSERT INTO processed_events (execution_id, event_id) VALUES (%s, %s) "
                        "ON CONFLICT DO NOTHING",
                        (exe.id, processed_event_id),
                    )
                for child_id, root_path, context in spawns:
                    cur.execute(
                        "INSERT INTO spawns (parent_id, child_id, root_path, context) "
                        "VALUES (%s, %s, %s, %s)",
                        (exe.id, child_id, root_path, json.dumps(context)),
                    )
                for op in timers:
                    if op.action == "schedule":
                        cur.execute(
                            "INSERT INTO timers (execution_id, path, fire_at) VALUES (%s, %s, %s) "
                            "ON CONFLICT (execution_id, path) DO UPDATE SET fire_at = EXCLUDED.fire_at",
                            (exe.id, op.path, op.fire_at),
                        )
                    else:
                        cur.execute(
                            "DELETE FROM timers WHERE execution_id = %s AND path = %s", (exe.id, op.path)
                        )
            self._conn.commit()
        except StoreConflict:
            raise
        except Exception:
            exe.version = old
            self._conn.rollback()
            raise

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM processed_events WHERE execution_id = %s AND event_id = %s",
                (execution_id, event_id),
            )
            found = cur.fetchone() is not None
        self._conn.commit()
        return found

    def pending_outbox(self) -> list[OutboxEntry]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT seq, target_id, event FROM outbox ORDER BY seq")
            rows = cur.fetchall()
        self._conn.commit()
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM outbox WHERE seq = %s", (seq,))
        self._conn.commit()

    def pending_spawns(self) -> list[SpawnEntry]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq")
            rows = cur.fetchall()
        self._conn.commit()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM spawns WHERE seq = %s", (seq,))
        self._conn.commit()

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= %s ORDER BY fire_at", (now,)
            )
            rows = cur.fetchall()
        self._conn.commit()
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM timers WHERE execution_id = %s AND path = %s AND fire_at = %s",
                (execution_id, path, fire_at),
            )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class RqliteStore:
    """A durable `ExecutionStore` over **rqlite** — distributed SQLite with Raft
    (HA, strong reads), spoken over its HTTP API. Same contract as SqliteStore.

    rqlite has no interactive (multi-roundtrip) transactions, so `commit` is one
    transactional request whose writes are all **guarded on the CAS succeeding**:
    the Execution upsert applies only `WHERE version = old`, and each outbox/dedupe
    insert runs only `WHERE EXISTS(... version = new)`. So a version mismatch makes
    the whole request a no-op, detected by the upsert's `rows_affected == 0`
    (→ `StoreConflict`). Reads use `level=strong` (linearizable, via the leader)."""

    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        import requests

        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._execute(
            [
                "CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, "
                "data TEXT NOT NULL, version INTEGER NOT NULL)",
                "CREATE TABLE IF NOT EXISTS outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, "
                "event TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS processed_events (execution_id TEXT NOT NULL, event_id TEXT NOT NULL, "
                "PRIMARY KEY (execution_id, event_id))",
                "CREATE TABLE IF NOT EXISTS timers (execution_id TEXT NOT NULL, path TEXT NOT NULL, "
                "fire_at REAL NOT NULL, PRIMARY KEY (execution_id, path))",
                "CREATE TABLE IF NOT EXISTS spawns (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                "parent_id TEXT NOT NULL, child_id TEXT NOT NULL, root_path TEXT NOT NULL, context TEXT NOT NULL)",
            ]
        )

    @classmethod
    def from_url(cls, url: str, connect_retries: int = 30, retry_delay: float = 1.0) -> "RqliteStore":
        """Build a store, retrying until rqlite is up and has elected a leader (a
        worker starting alongside rqlite in compose waits rather than crashing)."""
        import time

        import requests

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(url)
            except requests.exceptions.RequestException as exc:
                last = exc
                time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("rqlite connect failed")

    def _execute(self, statements: list, transaction: bool = False) -> list:
        params = {"transaction": ""} if transaction else {}
        resp = self._session.post(
            f"{self._base}/db/execute", params=params, json=statements, timeout=self._timeout
        )
        resp.raise_for_status()
        results = resp.json()["results"]
        for res in results:
            if "error" in res:
                raise RuntimeError(f"rqlite execute error: {res['error']}")
        return results

    def _query(self, sql: str, params: tuple) -> list:
        resp = self._session.post(
            f"{self._base}/db/query", params={"level": "strong"}, json=[[sql, *params]], timeout=self._timeout
        )
        resp.raise_for_status()
        result = resp.json()["results"][0]
        if "error" in result:
            raise RuntimeError(f"rqlite query error: {result['error']}")
        return result.get("values") or []

    def load(self, execution_id: str) -> Optional[Execution]:
        rows = self._query("SELECT data FROM executions WHERE id = ?", (execution_id,))
        return Execution.model_validate_json(rows[0][0]) if rows else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # distributed SQLite: same json_extract projection as SqliteStore, over _query.
        where, params = ["1=1"], []
        if definition_id is not None:
            where.append("definition_id = ?")
            params.append(definition_id)
        if status is not None:
            statuses = [s.value for s in status]
            where.append(f"json_extract(data,'$.status') IN ({','.join('?' * len(statuses))})")
            params += statuses
        if roots_only:
            where.append("json_extract(data,'$.parent_id') IS NULL")
        off = _decode_offset(cursor)
        rows = self._query(
            "SELECT id, definition_id, version, json_extract(data,'$.status'), "
            "json_extract(data,'$.outcome'), json_extract(data,'$.active_path'), "
            "json_extract(data,'$.parent_id') FROM executions "
            f"WHERE {' AND '.join(where)} ORDER BY id LIMIT ? OFFSET ?",
            (*params, limit + 1, off),
        )
        items = [
            ExecutionSummary(
                id=r[0],
                definition_id=r[1],
                version=r[2],
                status=r[3],
                outcome=r[4],
                active_path=r[5],
                parent_id=r[6],
            )
            for r in rows[:limit]
        ]
        nxt = _encode_offset(off + limit) if len(rows) > limit else None
        return ExecutionPage(items=items, next_cursor=nxt)

    def save(self, exe: Execution) -> None:
        self.commit(exe, [])

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        old = exe.version
        exe.version = old + 1  # bump BEFORE dumping so the stored JSON carries the new version
        new = exe.version
        data = exe.model_dump_json()
        # the upsert applies only WHERE version=old; every other write is guarded on
        # the row holding our `data` — so a CAS miss leaves the whole txn a no-op
        statements: list = [
            [
                "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET data = excluded.data, version = excluded.version "
                "WHERE executions.version = ?",
                exe.id,
                exe.definition_id,
                data,
                new,
                old,
            ]
        ]
        # guard each side-write on the row holding *our* exact `data` (not just
        # version=new): that is true iff our upsert won the CAS, so a concurrent
        # writer that reached the same version with different state can't make our
        # outbox leak. (Two byte-identical writes are idempotent; the target dedupes.)
        for target_id, event in emits:
            statements.append(
                [
                    "INSERT INTO outbox (target_id, event) SELECT ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    target_id,
                    event.model_dump_json(),
                    exe.id,
                    data,
                ]
            )
        if processed_event_id is not None:
            statements.append(
                [
                    "INSERT OR IGNORE INTO processed_events (execution_id, event_id) SELECT ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    processed_event_id,
                    exe.id,
                    data,
                ]
            )
        # timer ops, also guarded on our data winning the CAS. Schedule = delete+insert
        # (upsert), so re-entry replaces the fire_at; cancel = delete.
        for op in timers:
            statements.append(
                [
                    "DELETE FROM timers WHERE execution_id = ? AND path = ? "
                    "AND EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    op.path,
                    exe.id,
                    data,
                ]
            )
            if op.action == "schedule":
                statements.append(
                    [
                        "INSERT INTO timers (execution_id, path, fire_at) SELECT ?, ?, ? "
                        "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                        exe.id,
                        op.path,
                        op.fire_at,
                        exe.id,
                        data,
                    ]
                )
        # spawns, guarded on our data winning the CAS (like the outbox)
        for child_id, root_path, context in spawns:
            statements.append(
                [
                    "INSERT INTO spawns (parent_id, child_id, root_path, context) SELECT ?, ?, ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    child_id,
                    root_path,
                    json.dumps(context),
                    exe.id,
                    data,
                ]
            )
        results = self._execute(statements, transaction=True)
        if results[0].get("rows_affected", 0) == 0:
            exe.version = old  # CAS missed: undo the in-memory bump (nothing was written)
            found = self._query("SELECT version FROM executions WHERE id = ?", (exe.id,))
            raise StoreConflict(exe.id, expected=old, found=found[0][0] if found else None)
        # success: exe.version is already `new`

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        rows = self._query(
            "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        )
        return bool(rows)

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._query("SELECT seq, target_id, event FROM outbox ORDER BY seq", ())
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._execute([["DELETE FROM outbox WHERE seq = ?", seq]])

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._query("SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq", ())
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        self._execute([["DELETE FROM spawns WHERE seq = ?", seq]])

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._query(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
        )
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._execute(
            [
                [
                    "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
                    execution_id,
                    path,
                    fire_at,
                ]
            ]
        )

    def close(self) -> None:
        self._session.close()


class MongoStore:
    """A durable `ExecutionStore` over MongoDB (pymongo) — the document-store
    alternative to the SQL backends, all-network (no shared filesystem). Same
    contract: version/CAS, transactional outbox, dedupe, spawns, timers.

    MongoDB has no multi-document transactions without a replica set, so instead
    of separate collections (as the SQL stores use separate tables) **everything
    for one Execution lives in its single document** — `data` (the serialized
    Execution), plus the `outbox`/`spawns` arrays, the `timers` sub-document and
    the `processed` array. A `commit` is therefore **one `update_one`**, which is
    atomic on a single document with no replica set required — the whole point of
    embedding here.

    Performance note: we never round-trip the whole document. Writes are partial —
    `$set` of `data`/`version`, `$push` to the arrays, `$addToSet`/`$set`/`$unset`
    on the rest — never a full `replace_one`. Reads are projected — `load` pulls
    only `data`; the relay/sweep reads (`pending_outbox`/`pending_spawns`/
    `due_timers`) project only the relevant array/sub-document, never `data`. So a
    growing `data` blob is not dragged through every queue/timer scan.

    The client is injected (duck-typed), so `pymongo` stays an optional dependency
    and tests use mongomock. Collections live under `db_name`: ``executions`` (the
    documents) + ``counters`` (the monotonic outbox/spawn seq allocator)."""

    def __init__(self, client: Any, db_name: str = "harel") -> None:
        from pymongo import ReturnDocument
        from pymongo.errors import DuplicateKeyError

        self._client = client
        self._db = client[db_name]
        self._exes = self._db["executions"]
        self._counters = self._db["counters"]
        self._after = ReturnDocument.AFTER
        self._DuplicateKeyError = DuplicateKeyError

    @classmethod
    def from_url(
        cls, url: str, db_name: str = "harel", connect_retries: int = 30, retry_delay: float = 1.0
    ) -> "MongoStore":
        """Convenience constructor; imports `pymongo` lazily (the optional dep).
        Pings the server, retrying so a worker starting alongside Mongo (compose)
        waits for it to accept connections rather than crashing."""
        import time

        import pymongo
        from pymongo.errors import PyMongoError

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                client: Any = pymongo.MongoClient(url)
                client.admin.command("ping")
                return cls(client, db_name)
            except PyMongoError as exc:
                last = exc
                time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("mongo connect failed")

    # Node paths use "." as the separator (`Fork.A`); Mongo treats "." in a field
    # name as a path operator, so encode it to a char that cannot appear in a path
    # before using a path as a `timers` sub-document key (reversible).
    @staticmethod
    def _enc(path: str) -> str:
        return path.replace(".", "．")

    @staticmethod
    def _dec(key: str) -> str:
        return key.replace("．", ".")

    def _next_seq(self, name: str, count: int) -> int:
        """Reserve `count` monotonic ids from the `name` counter; return the first.
        A block wasted by a later-aborted commit is harmless (gaps are fine)."""
        doc = self._counters.find_one_and_update(
            {"_id": name}, {"$inc": {"n": count}}, upsert=True, return_document=self._after
        )
        return int(doc["n"]) - count + 1

    def load(self, execution_id: str) -> Optional[Execution]:
        doc = self._exes.find_one({"_id": execution_id}, {"data": 1})
        return Execution.model_validate_json(doc["data"]) if doc is not None else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # definition_id is a top-level field (filtered server-side); status/parent_id
        # live inside the `data` JSON string (filtered client-side). Project only
        # {definition_id, version, data} — never the embedded outbox/spawns/timers arrays.
        status = set(status) if status is not None else None
        query: dict = {} if definition_id is None else {"definition_id": definition_id}
        off = _decode_offset(cursor)
        items: list[ExecutionSummary] = []
        # over-fetch so client-side status/roots filtering still fills the page; if we
        # exhaust the cursor short of `limit`, there simply is no next page.
        cur = self._exes.find(query, {"version": 1, "data": 1}).sort("_id", 1).skip(off)
        scanned = 0
        for doc in cur:
            scanned += 1
            summary = ExecutionSummary.from_data(json.loads(doc["data"]), doc.get("version", 0))
            if _matches(summary, status, definition_id, roots_only):
                items.append(summary)
            if len(items) >= limit:
                break
        nxt = _encode_offset(off + scanned) if len(items) >= limit else None
        return ExecutionPage(items=items, next_cursor=nxt)

    def save(self, exe: Execution) -> None:
        self.commit(exe, [])

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        # allocate monotonic seqs up front (one find_one_and_update each; a seq
        # wasted by a lost CAS is harmless), then build the embedded entries
        outbox_entries: list[dict] = []
        if emits:
            base = self._next_seq("outbox", len(emits))
            outbox_entries = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn_entries: list[dict] = []
        if spawns:
            base = self._next_seq("spawn", len(spawns))
            spawn_entries = [
                {"seq": base + i, "parent_id": exe.id, "child_id": cid, "root_path": rp, "context": dict(ctx)}
                for i, (cid, rp, ctx) in enumerate(spawns)
            ]

        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()

        # partial write: $set the core, $push the queues, $addToSet dedupe, and
        # $set/$unset the timer keys — NEVER replace_one the whole document
        set_ops: dict[str, Any] = {"data": data, "version": exe.version}
        unset_ops: dict[str, str] = {}
        for op in timers:
            key = f"timers.{self._enc(op.path)}"
            if op.action == "schedule":
                set_ops[key] = op.fire_at
            else:
                unset_ops[key] = ""
        update: dict[str, Any] = {"$set": set_ops}
        push: dict[str, Any] = {}
        if outbox_entries:
            push["outbox"] = {"$each": outbox_entries}
        if spawn_entries:
            push["spawns"] = {"$each": spawn_entries}
        if push:
            update["$push"] = push
        if processed_event_id is not None:
            update["$addToSet"] = {"processed": processed_event_id}
        if unset_ops:
            update["$unset"] = unset_ops

        res = self._exes.update_one({"_id": exe.id, "version": old}, update)
        if res.matched_count == 1:
            return  # CAS won

        # no document at version=old: either a brand-new Execution or a stale write
        existing = self._exes.find_one({"_id": exe.id}, {"version": 1})
        if existing is None and old == 0:
            doc: dict[str, Any] = {
                "_id": exe.id,
                "definition_id": exe.definition_id,
                "version": exe.version,
                "data": data,
                "outbox": outbox_entries,
                "spawns": spawn_entries,
                "processed": [processed_event_id] if processed_event_id is not None else [],
                "timers": {self._enc(op.path): op.fire_at for op in timers if op.action == "schedule"},
            }
            try:
                self._exes.insert_one(doc)
                return
            except self._DuplicateKeyError:
                existing = self._exes.find_one({"_id": exe.id}, {"version": 1})
        exe.version = old  # undo the in-memory bump; the commit did not happen
        raise StoreConflict(exe.id, expected=old, found=existing["version"] if existing else None)

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        return self._exes.find_one({"_id": execution_id, "processed": event_id}, {"_id": 1}) is not None

    def pending_outbox(self) -> list[OutboxEntry]:
        entries: list[OutboxEntry] = []
        # project only the outbox array (never `data`), oldest first by seq
        for doc in self._exes.find({"outbox": {"$exists": True, "$ne": []}}, {"outbox": 1}):
            for e in doc.get("outbox", []):
                entries.append(OutboxEntry(e["seq"], e["target_id"], Event.model_validate_json(e["event"])))
        return sorted(entries, key=lambda e: e.seq)

    def ack_outbox(self, seq: int) -> None:
        self._exes.update_one({"outbox.seq": seq}, {"$pull": {"outbox": {"seq": seq}}})

    def pending_spawns(self) -> list[SpawnEntry]:
        entries: list[SpawnEntry] = []
        for doc in self._exes.find({"spawns": {"$exists": True, "$ne": []}}, {"spawns": 1}):
            for s in doc.get("spawns", []):
                entries.append(
                    SpawnEntry(s["seq"], s["parent_id"], s["child_id"], s["root_path"], dict(s["context"]))
                )
        return sorted(entries, key=lambda s: s.seq)

    def ack_spawn(self, seq: int) -> None:
        self._exes.update_one({"spawns.seq": seq}, {"$pull": {"spawns": {"seq": seq}}})

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        out: list[tuple[str, str, float]] = []
        # project only the timers sub-document (never `data`)
        for doc in self._exes.find({"timers": {"$exists": True, "$ne": {}}}, {"timers": 1}):
            for enc, fire_at in (doc.get("timers") or {}).items():
                if fire_at <= now:
                    out.append((doc["_id"], self._dec(enc), float(fire_at)))
        return sorted(out, key=lambda t: t[2])

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        # guarded on the stored value: a concurrent re-schedule to a new time wins
        key = f"timers.{self._enc(path)}"
        self._exes.update_one({"_id": execution_id, key: fire_at}, {"$unset": {key: ""}})

    def close(self) -> None:
        self._client.close()


class DynamoDBStore:
    """A durable `ExecutionStore` over AWS DynamoDB (boto3) — the serverless
    sibling of the SQL backends, and the natural store-side partner of
    `SqsTransport` for an **all-AWS, no-server** stack. Runs against real DynamoDB
    or **LocalStack**/**moto** (no AWS account); the client is injected, so `boto3`
    stays an optional extra.

    DynamoDB gives the two primitives directly: conditional writes are the CAS
    (`attribute_not_exists(id)` to insert, `version = :old` to update) and
    `TransactWriteItems` makes the whole `commit` atomic across items — the
    Execution Put (CAS-conditioned) plus the outbox/spawns/processed/timers writes
    either all apply or none, so a stale write never leaks its outbox. A failed CAS
    cancels the transaction (`TransactionCanceledException`; older moto raises
    `ConditionalCheckFailedException`, so we treat both as a conflict).

    Tables (created idempotently, prefixed by `prefix`): ``executions`` (id),
    ``outbox``/``spawns`` (seq), ``timers`` (execution_id, path), ``processed``
    (execution_id, event_id), ``counters`` (the monotonic seq allocator).

    Trade-offs of the document model, deliberately accepted: the relay/sweep reads
    (`pending_outbox`/`pending_spawns`/`due_timers`) use `Scan` + a client-side
    sort (DynamoDB Scan is unordered) — fine because those tables drain and stay
    near-empty. `TransactWriteItems` caps a single commit at 100 items / 4MB, far
    above a normal commit (1 Execution + a handful of emits/spawns/timers)."""

    def __init__(self, client: Any, prefix: str = "harel") -> None:
        from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
        from botocore.exceptions import ClientError

        self._db = client
        self._prefix = prefix
        self._ser = TypeSerializer()
        self._deser = TypeDeserializer()
        self._ClientError = ClientError
        self._ensure_tables()

    @classmethod
    def create(
        cls,
        endpoint_url: Optional[str] = None,
        region: str = "us-east-1",
        prefix: str = "harel",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "DynamoDBStore":
        """Build a boto3 client (LocalStack-friendly: dummy creds + injected
        `endpoint_url`; pass `endpoint_url=None` for real AWS) and ensure the
        tables exist, retrying until the endpoint is reachable."""
        import time

        import boto3
        from botocore.exceptions import BotoCoreError, ClientError

        kwargs: dict[str, Any] = {"region_name": region}
        if endpoint_url is not None:
            kwargs.update(endpoint_url=endpoint_url, aws_access_key_id="test", aws_secret_access_key="test")
        client = boto3.client("dynamodb", **kwargs)
        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(client, prefix)
            except (BotoCoreError, ClientError) as exc:
                last = exc
                time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("dynamodb connect failed")

    def _t(self, name: str) -> str:
        return f"{self._prefix}_{name}"

    def _ensure_tables(self) -> None:
        """Create the tables if absent (idempotent — a pre-existing table is fine).
        Convenient for LocalStack/moto/dev; on real AWS the tables usually pre-exist."""
        specs = [
            ("executions", [("id", "S")]),
            ("outbox", [("seq", "N")]),
            ("spawns", [("seq", "N")]),
            ("timers", [("execution_id", "S"), ("path", "S")]),
            ("processed", [("execution_id", "S"), ("event_id", "S")]),
            ("counters", [("id", "S")]),
        ]
        roles = ["HASH", "RANGE"]
        for name, keys in specs:
            try:
                self._db.create_table(
                    TableName=self._t(name),
                    KeySchema=[{"AttributeName": k, "KeyType": roles[i]} for i, (k, _) in enumerate(keys)],
                    AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t in keys],
                    BillingMode="PAY_PER_REQUEST",
                )
            except self._ClientError as exc:
                if exc.response["Error"]["Code"] != "ResourceInUseException":
                    raise  # already exists is fine; anything else is real

    def _raw(self, item: dict) -> dict:
        """A native dict → DynamoDB's typed attribute-value form."""
        return {k: self._ser.serialize(v) for k, v in item.items()}

    def _item(self, raw: dict) -> dict:
        """DynamoDB's typed form → a native dict (numbers come back as Decimal)."""
        return {k: self._deser.deserialize(v) for k, v in raw.items()}

    def _scan(self, table: str, **params: Any) -> list[dict]:
        """Scan a table, following `LastEvaluatedKey` to drain every page (a single Scan
        returns at most 1MB). `params` adds scan options such as a `FilterExpression`."""
        items: list[dict] = []
        kwargs: dict[str, Any] = {"TableName": self._t(table), **params}
        while True:
            resp = self._db.scan(**kwargs)
            items.extend(self._item(it) for it in resp.get("Items", []))
            start = resp.get("LastEvaluatedKey")
            if not start:
                return items
            kwargs["ExclusiveStartKey"] = start

    def _next_seq(self, name: str, count: int) -> int:
        """Reserve `count` monotonic ids from the `name` counter (an atomic ADD);
        return the first. A block wasted by a later-cancelled transaction is harmless."""
        resp = self._db.update_item(
            TableName=self._t("counters"),
            Key=self._raw({"id": name}),
            UpdateExpression="ADD n :k",
            ExpressionAttributeValues={":k": {"N": str(count)}},
            ReturnValues="UPDATED_NEW",
        )
        return int(resp["Attributes"]["n"]["N"]) - count + 1

    def load(self, execution_id: str) -> Optional[Execution]:
        resp = self._db.get_item(
            TableName=self._t("executions"),
            Key=self._raw({"id": execution_id}),
            ProjectionExpression="#d",
            ExpressionAttributeNames={"#d": "data"},
        )
        item = resp.get("Item")
        return Execution.model_validate_json(self._item(item)["data"]) if item else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # Scan (unordered) projecting only data+version; status/parent_id are inside the
        # `data` JSON so they're filtered client-side (definition_id can be a server-side
        # FilterExpression). cursor = base64 of the native LastEvaluatedKey. `Limit` bounds
        # items EXAMINED before the filter, so a page may return fewer than `limit` matches.
        status = set(status) if status is not None else None
        kwargs: dict[str, Any] = {
            "TableName": self._t("executions"),
            "ProjectionExpression": "#dat,#v",
            "ExpressionAttributeNames": {"#dat": "data", "#v": "version"},
            "Limit": limit,
        }
        if definition_id is not None:
            kwargs["ExpressionAttributeNames"]["#def"] = "definition_id"
            kwargs["FilterExpression"] = "#def = :def"
            kwargs["ExpressionAttributeValues"] = {":def": {"S": definition_id}}
        if cursor:
            kwargs["ExclusiveStartKey"] = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        resp = self._db.scan(**kwargs)
        items = []
        for raw in resp.get("Items", []):
            item = self._item(raw)
            summary = ExecutionSummary.from_data(json.loads(item["data"]), int(item.get("version", 0)))
            if _matches(summary, status, definition_id, roots_only):
                items.append(summary)
        lek = resp.get("LastEvaluatedKey")
        nxt = base64.urlsafe_b64encode(json.dumps(lek).encode()).decode() if lek else None
        return ExecutionPage(items=items, next_cursor=nxt)

    def save(self, exe: Execution) -> None:
        self.commit(exe, [])

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        # allocate monotonic seqs up front (a seq wasted by a cancelled txn is harmless)
        outbox: list[dict] = []
        if emits:
            base = self._next_seq("outbox", len(emits))
            outbox = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn: list[dict] = []
        if spawns:
            base = self._next_seq("spawn", len(spawns))
            spawn = [
                {
                    "seq": base + i,
                    "parent_id": exe.id,
                    "child_id": cid,
                    "root_path": rp,
                    "context": json.dumps(ctx),
                }
                for i, (cid, rp, ctx) in enumerate(spawns)
            ]

        old = exe.version
        exe.version = old + 1
        exe_item = {
            "id": exe.id,
            "data": exe.model_dump_json(),
            "version": exe.version,
            "definition_id": exe.definition_id,
        }

        # the Execution Put carries the CAS: insert iff absent (old==0), else update
        # iff the stored version still matches — a failed condition cancels the txn
        if old == 0:
            cas: dict[str, Any] = {"ConditionExpression": "attribute_not_exists(id)"}
        else:
            cas = {
                "ConditionExpression": "version = :ov",
                "ExpressionAttributeValues": {":ov": {"N": str(old)}},
            }
        txn: list[dict] = [{"Put": {"TableName": self._t("executions"), "Item": self._raw(exe_item), **cas}}]
        for o in outbox:
            txn.append({"Put": {"TableName": self._t("outbox"), "Item": self._raw(o)}})
        for s in spawn:
            txn.append({"Put": {"TableName": self._t("spawns"), "Item": self._raw(s)}})
        if processed_event_id is not None:
            txn.append(
                {
                    "Put": {
                        "TableName": self._t("processed"),
                        "Item": self._raw({"execution_id": exe.id, "event_id": processed_event_id}),
                    }
                }
            )
        for op in timers:
            if op.action == "schedule":
                txn.append(
                    {
                        "Put": {
                            "TableName": self._t("timers"),
                            "Item": self._raw(
                                {"execution_id": exe.id, "path": op.path, "fire_at": Decimal(str(op.fire_at))}
                            ),
                        }
                    }
                )
            else:
                txn.append(
                    {
                        "Delete": {
                            "TableName": self._t("timers"),
                            "Key": self._raw({"execution_id": exe.id, "path": op.path}),
                        }
                    }
                )

        try:
            self._db.transact_write_items(TransactItems=txn)
        except self._ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in ("TransactionCanceledException", "ConditionalCheckFailedException"):
                raise  # a real error, not a CAS miss
            exe.version = old  # undo the in-memory bump; the txn was cancelled
            resp = self._db.get_item(
                TableName=self._t("executions"),
                Key=self._raw({"id": exe.id}),
                ProjectionExpression="version",
            )
            found = int(self._item(resp["Item"])["version"]) if "Item" in resp else None
            raise StoreConflict(exe.id, expected=old, found=found)

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        resp = self._db.get_item(
            TableName=self._t("processed"),
            Key=self._raw({"execution_id": execution_id, "event_id": event_id}),
        )
        return "Item" in resp

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._scan("outbox")
        rows.sort(key=lambda r: int(r["seq"]))  # Scan is unordered; sort by seq
        return [
            OutboxEntry(int(r["seq"]), r.get("target_id"), Event.model_validate_json(r["event"]))
            for r in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._db.delete_item(TableName=self._t("outbox"), Key=self._raw({"seq": seq}))

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._scan("spawns")
        rows.sort(key=lambda r: int(r["seq"]))
        return [
            SpawnEntry(int(r["seq"]), r["parent_id"], r["child_id"], r["root_path"], json.loads(r["context"]))
            for r in rows
        ]

    def ack_spawn(self, seq: int) -> None:
        self._db.delete_item(TableName=self._t("spawns"), Key=self._raw({"seq": seq}))

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._scan(
            "timers",
            FilterExpression="fire_at <= :now",
            ExpressionAttributeValues={":now": {"N": str(now)}},
        )
        out = [(r["execution_id"], r["path"], float(r["fire_at"])) for r in rows]
        return sorted(out, key=lambda t: t[2])

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        # guarded on the stored value: a concurrent re-schedule to a new time wins
        try:
            self._db.delete_item(
                TableName=self._t("timers"),
                Key=self._raw({"execution_id": execution_id, "path": path}),
                ConditionExpression="fire_at = :f",
                ExpressionAttributeValues={":f": {"N": str(Decimal(str(fire_at)))}},
            )
        except self._ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise  # the guard didn't match (stale sweep) — a no-op, as intended

    def close(self) -> None:
        self._db.close()
