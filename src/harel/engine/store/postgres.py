"""PostgresStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.store._base import (
    DEFAULT_TRACE_MAX,
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    _decode_offset,
    _encode_offset,
)
from harel.spec.states import Event


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
            cur.execute(
                "CREATE TABLE IF NOT EXISTS trace "
                "(execution_id TEXT NOT NULL, idx INT NOT NULL, entry TEXT NOT NULL, "
                "PRIMARY KEY (execution_id, idx))"
            )
        self.trace_max = DEFAULT_TRACE_MAX
        conn.commit()

    def _write_trace(self, cur: Any, execution_id: str, entry: dict) -> None:
        """Append one trace step on the given cursor (inside commit's txn). Two statements:
        `idx` computed inline (MAX+1, monotonic) so no pre-read, then the ring cap. `read_trace`
        takes `index` from the `idx` column."""
        cur.execute(
            "INSERT INTO trace (execution_id, idx, entry) "
            "SELECT %s, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = %s), -1) + 1, %s",
            (execution_id, execution_id, json.dumps(entry)),
        )
        if self.trace_max:
            cur.execute(
                "DELETE FROM trace WHERE execution_id = %s AND idx <= "
                "(SELECT MAX(idx) FROM trace WHERE execution_id = %s) - %s",
                (execution_id, execution_id, self.trace_max),
            )

    def append_trace(self, execution_id: str, entry: dict) -> None:
        with self._conn.cursor() as cur:
            self._write_trace(cur, execution_id, entry)
        self._conn.commit()

    def read_trace(self, execution_id: str) -> list[dict]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT idx, entry FROM trace WHERE execution_id = %s ORDER BY idx", (execution_id,))
            rows = cur.fetchall()
        self._conn.commit()
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

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
        trace: Optional[dict] = None,
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
                if trace is not None:
                    self._write_trace(cur, exe.id, trace)
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
