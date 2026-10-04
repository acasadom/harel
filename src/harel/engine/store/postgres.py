"""PostgresStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, Status
from harel.engine.schema import DEFAULT_PREFIX, Names, store_schema
from harel.engine.store._base import (
    _IDS_WITH_PREFIX_SQL,
    _PG_SCHEMA_LOCK,
    _PURGE_COMPANIONS_SQL,
    DEFAULT_TRACE_MAX,
    OutboxEntry,
    SpawnEntry,
    Step,
    StoreConflict,
    TimerOp,
    _decode_offset,
    _like_prefix,
    _listing_page,
    _listing_sql_pg,
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

    def __init__(self, conn: Any, *, prefix: str = DEFAULT_PREFIX, create_schema: bool = True) -> None:
        """`prefix` names its tables and functions (see `harel.engine.schema`); with
        `create_schema=False` they must already exist."""
        self._n = Names(prefix)
        self._conn = conn
        if create_schema:
            with conn.cursor() as cur:
                # serialize concurrent schema setup (CREATE OR REPLACE FUNCTION rewrites pg_proc)
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (_PG_SCHEMA_LOCK,))
                for sql in store_schema("postgres", prefix):
                    cur.execute(sql)
            conn.commit()
        self.trace_max = DEFAULT_TRACE_MAX

    def _write_trace(self, cur: Any, execution_id: str, entry: dict) -> None:
        """Append one trace step on the given cursor (inside commit's txn). Two statements:
        `idx` computed inline (MAX+1, monotonic) so no pre-read, then the ring cap. `read_trace`
        takes `index` from the `idx` column."""
        cur.execute(
            f"INSERT INTO {self._n.trace} (execution_id, idx, entry) "
            f"SELECT %s, COALESCE((SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = %s), -1) + 1, %s",
            (execution_id, execution_id, json.dumps(entry)),
        )
        if self.trace_max:
            cur.execute(
                f"DELETE FROM {self._n.trace} WHERE execution_id = %s AND idx <= "
                f"(SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = %s) - %s",
                (execution_id, execution_id, self.trace_max),
            )

    def append_trace(self, execution_id: str, entry: dict) -> None:
        with self._conn.cursor() as cur:
            self._write_trace(cur, execution_id, entry)
        self._conn.commit()

    def read_trace(self, execution_id: str) -> list[dict]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT idx, entry FROM {self._n.trace} WHERE execution_id = %s ORDER BY idx",
                (execution_id,),
            )
            rows = cur.fetchall()
        self._conn.commit()
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

    @classmethod
    def from_dsn(
        cls,
        dsn: str,
        connect_retries: int = 15,
        retry_delay: float = 1.0,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> "PostgresStore":
        """Convenience constructor; imports `psycopg` lazily (the optional dep).
        Retries the connection so a worker starting alongside Postgres (compose)
        waits for it to accept connections rather than crashing."""
        import time

        import psycopg

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(psycopg.connect(dsn), prefix=prefix, create_schema=create_schema)
            except psycopg.OperationalError as exc:
                last = exc
                time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("postgres connect failed")

    def load(self, execution_id: str) -> Optional[Execution]:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT data FROM {self._n.executions} WHERE id = %s", (execution_id,))
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
        off = _decode_offset(cursor)
        sql, params = _listing_sql_pg(self._n.executions, status, definition_id, roots_only, limit, off)
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        self._conn.commit()  # end the read transaction
        return _listing_page(rows, limit, off)

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
        step: Optional[Step] = None,
    ) -> list[int]:
        # fast path: a state-only event (no emits/spawns/timers/trace) commits in ONE atomic
        # round-trip via the version-CAS function — instead of UPDATE + (SELECT/INSERT) + INSERT.
        if not emits and not spawns and not timers and trace is None:
            self._commit_cas(exe, processed_event_id)
            return []
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {self._n.executions} SET data = %s, version = %s WHERE id = %s AND version = %s",
                    (data, exe.version, exe.id, old),
                )
                if cur.rowcount == 0:
                    cur.execute(f"SELECT version FROM {self._n.executions} WHERE id = %s", (exe.id,))
                    row = cur.fetchone()
                    if row is None and old == 0:
                        cur.execute(
                            f"INSERT INTO {self._n.executions} (id, definition_id, data, version) VALUES (%s, %s, %s, %s)",
                            (exe.id, exe.definition_id, data, exe.version),
                        )
                    else:
                        exe.version = old
                        self._conn.rollback()
                        raise StoreConflict(exe.id, expected=old, found=row[0] if row else None)
                seqs = []
                for target_id, event in emits:
                    cur.execute(
                        f"INSERT INTO {self._n.outbox} (target_id, event) VALUES (%s, %s) RETURNING seq",
                        (target_id, event.model_dump_json()),
                    )
                    seqs.append(cur.fetchone()[0])
                if processed_event_id is not None:
                    cur.execute(
                        f"INSERT INTO {self._n.processed_events} (execution_id, event_id) VALUES (%s, %s) "
                        "ON CONFLICT DO NOTHING",
                        (exe.id, processed_event_id),
                    )
                for child_id, root_path, context in spawns:
                    cur.execute(
                        f"INSERT INTO {self._n.spawns} (parent_id, child_id, root_path, context) "
                        "VALUES (%s, %s, %s, %s)",
                        (exe.id, child_id, root_path, json.dumps(context)),
                    )
                for op in timers:
                    if op.action == "schedule":
                        cur.execute(
                            f"INSERT INTO {self._n.timers} (execution_id, path, fire_at) VALUES (%s, %s, %s) "
                            "ON CONFLICT (execution_id, path) DO UPDATE SET fire_at = EXCLUDED.fire_at",
                            (exe.id, op.path, op.fire_at),
                        )
                    else:
                        cur.execute(
                            f"DELETE FROM {self._n.timers} WHERE execution_id = %s AND path = %s",
                            (exe.id, op.path),
                        )
                if trace is not None:
                    self._write_trace(cur, exe.id, trace)
            self._conn.commit()
            return seqs
        except StoreConflict:
            raise
        except Exception:
            exe.version = old
            self._conn.rollback()
            raise

    def _commit_cas(self, exe: Execution, processed_event_id: Optional[str]) -> None:
        """The fast-path commit: version-CAS + write (+ dedupe) in one atomic round-trip via
        the `commit_cas` function (`schema.Names.commit_cas`). Returns false on a version conflict (no RAISE, so the txn is clean)."""
        old = exe.version
        exe.version = old + 1
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT {self._n.commit_cas}(%s, %s, %s, %s, %s)",
                (exe.id, exe.definition_id, exe.model_dump_json(), old, processed_event_id or ""),
            )
            ok = cur.fetchone()[0]
        if not ok:
            exe.version = old
            with self._conn.cursor() as cur2:
                cur2.execute(f"SELECT version FROM {self._n.executions} WHERE id = %s", (exe.id,))
                row = cur2.fetchone()
            self._conn.rollback()
            raise StoreConflict(exe.id, expected=old, found=row[0] if row else None)
        self._conn.commit()

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {self._n.processed_events} WHERE execution_id = %s AND event_id = %s",
                (execution_id, event_id),
            )
            found = cur.fetchone() is not None
        self._conn.commit()
        return found

    def pending_outbox(self) -> list[OutboxEntry]:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT seq, target_id, event FROM {self._n.outbox} ORDER BY seq")
            rows = cur.fetchall()
        self._conn.commit()
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        with self._conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self._n.outbox} WHERE seq = %s", (seq,))
        self._conn.commit()

    def pending_spawns(self) -> list[SpawnEntry]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT seq, parent_id, child_id, root_path, context FROM {self._n.spawns} ORDER BY seq"
            )
            rows = cur.fetchall()
        self._conn.commit()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        with self._conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self._n.spawns} WHERE seq = %s", (seq,))
        self._conn.commit()

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT execution_id, path, fire_at FROM {self._n.timers} WHERE fire_at <= %s ORDER BY fire_at",
                (now,),
            )
            rows = cur.fetchall()
        self._conn.commit()
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._n.timers} WHERE execution_id = %s AND path = %s AND fire_at = %s",
                (execution_id, path, fire_at),
            )
        self._conn.commit()

    def ids_with_prefix(self, prefix: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(self._n.sql(_IDS_WITH_PREFIX_SQL).replace("?", "%s"), (_like_prefix(prefix),))
            rows = cur.fetchall()
        self._conn.commit()  # end the read transaction
        return [r[0] for r in rows]

    def purge(self, execution_id: str, expected_version: int) -> bool:
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self._n.executions} WHERE id = %s AND version = %s",
                    (execution_id, expected_version),
                )
                deleted = cur.rowcount
                if not deleted:
                    cur.execute(f"SELECT 1 FROM {self._n.executions} WHERE id = %s", (execution_id,))
                    if cur.fetchone() is not None:
                        self._conn.rollback()
                        return False  # moved on: touch nothing
                for sql in map(self._n.sql, _PURGE_COMPANIONS_SQL):
                    cur.execute(sql.replace("?", "%s"), (execution_id,))
            self._conn.commit()
            return bool(deleted)
        except BaseException:
            self._conn.rollback()
            raise

    def close(self) -> None:
        self._conn.close()
