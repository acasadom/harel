"""The event transport: a queue with **single-active-consumer per group**.

A `Transport` carries events to the Execution they belong to. All events for one
Execution share a `group_id` (= the execution id), and the transport hands out at
most **one in-flight message per group** at a time: until a worker `ack`s the
message it holds for group G, no worker receives another message of G. So each
Execution is processed by at most one worker at a time (the single-writer the
durable store's CAS assumes), while different groups run concurrently — overall
concurrency is just the number of workers. Order within a group is FIFO.

Four primitives hide all of that: `publish` (enqueue), `claim` (lease the next
deliverable message), `ack` (done — frees the group), `nack` (return it — now, or
*parked* for `delay` seconds; the control plane parks a suspended group's message
so a paused Execution does not spin a worker). A claimed message carries a
**lease** with a visibility timeout: if the worker dies, the lease expires and the
message becomes claimable again, so no group is blocked forever by a crash.

Backends differ only in *how* they enforce per-group exclusivity:
- `InMemoryTransport`: a lock serializes claims (same process).
- `SqliteTransport`: `BEGIN IMMEDIATE` takes SQLite's global write-lock, which
  serializes all claims — so the "oldest message of a group with nothing
  in-flight" selection is race-free without row locks or advisory locks. Works
  for processes on one machine, and unchanged over a single-writer distributed
  SQLite (rqlite/dqlite/LiteFS) where the leader serializes writes.
- `RedisTransport`: Redis has no native message groups, so the per-group lock is
  built by hand — `SET lock:{G} NX PX` is the group lock, a list per group is the
  FIFO, the lock's TTL is the lease. The client is injected (duck-typed), so tests
  use fakeredis and `redis` stays an optional dependency.
- `PostgresTransport`: a queue table; `claim` takes a single global advisory lock
  to serialize claims (like SQLite's write-lock) → race-free per-group exclusivity.
- `RqliteTransport`: a queue table on distributed SQLite; Raft serializes writes,
  so `claim` is one token-leasing UPDATE. Multi-machine, no Redis.

- `SqsTransport`: AWS SQS **FIFO** — the native fit; `MessageGroupId` is the
  per-group exclusivity and the visibility timeout is the lease. Runs on LocalStack.

Store and transport are independent seams: a deployment can mix them (e.g.
PostgresStore + RedisTransport) or unify on one backend (all-postgres, all-rqlite).
"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Protocol, Union, runtime_checkable

from harel.spec.states import Event

# sentinel `locked_by` for a message parked by `nack(delay>0)`: non-null (so the
# claim's "available"/in-flight checks skip it) until its `lock_expiry` passes.
_PARKED = "__parked__"


@dataclass
class Lease:
    """A claimed message: the `group_id` it belongs to and the `event`, plus the
    backend's handle to identify it on ack/nack — `seq` (the row/message id, for
    the in-memory and sqlite backends) or `token` (the Redis group-lock fencing
    token). Held until `ack` (delivered) or `nack`/expiry (re-deliver)."""

    seq: int
    group_id: str
    event: Event
    token: str = ""


@runtime_checkable
class Transport(Protocol):
    def publish(self, group_id: str, event: Event) -> None:
        """Enqueue `event` in `group_id`'s FIFO."""
        ...

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        """Lease the oldest message of some group that has nothing in-flight, for
        `visibility` seconds; None if there is nothing deliverable right now."""
        ...

    def ack(self, lease: Lease) -> None:
        """The message was handled: remove it, freeing its group."""
        ...

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        """Return the message to the queue. With `delay=0` it is immediately
        claimable again (retry now); with `delay>0` it is *parked* — not claimable
        (and its group stays blocked) until `delay` seconds pass. Parking lets a
        worker bounce a suspended group's message without spinning on it."""
        ...

    def close(self) -> None:
        """Release any backend resources (connection/client/session)."""
        ...


class InMemoryTransport:
    """Same-process `Transport`: a list guarded by a lock. The lock serializes
    `claim` (so the per-group exclusivity check is race-free), mirroring what the
    SQLite write-lock does across processes."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._messages: list[dict] = []
        self._seq = 0
        self._lock = threading.Lock()
        self._clock = clock

    def publish(self, group_id: str, event: Event) -> None:
        with self._lock:
            self._seq += 1
            self._messages.append(
                {
                    "seq": self._seq,
                    "group_id": group_id,
                    "event": event,
                    "locked_by": None,
                    "lock_expiry": 0.0,
                }
            )

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        with self._lock:
            in_flight = {
                m["group_id"]
                for m in self._messages
                if m["locked_by"] is not None and m["lock_expiry"] >= now
            }
            for m in sorted(self._messages, key=lambda m: m["seq"]):
                available = m["locked_by"] is None or m["lock_expiry"] < now
                if available and m["group_id"] not in in_flight:
                    m["locked_by"] = worker_id
                    m["lock_expiry"] = now + visibility
                    return Lease(m["seq"], m["group_id"], m["event"])
            return None

    def ack(self, lease: Lease) -> None:
        with self._lock:
            self._messages = [m for m in self._messages if m["seq"] != lease.seq]

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        with self._lock:
            for m in self._messages:
                if m["seq"] == lease.seq:
                    if delay > 0:
                        m["locked_by"] = _PARKED
                        m["lock_expiry"] = self._clock() + delay
                    else:
                        m["locked_by"] = None
                        m["lock_expiry"] = 0.0

    def close(self) -> None:
        pass  # nothing to release; the list lives with the process


class SqliteTransport:
    """Durable `Transport` over SQLite. `claim` runs inside `BEGIN IMMEDIATE`, so
    SQLite's global write-lock serializes claims across processes — the per-group
    exclusivity selection is then race-free with plain SQL (no row/advisory
    locks). One connection per thread/process on the same file (WAL mode); the
    lease (`lock_expiry`) recovers a message a crashed worker was holding."""

    def __init__(self, path: Union[str, Path] = ":memory:", clock: Callable[[], float] = time.time) -> None:
        # isolation_level=None -> autocommit; we drive BEGIN IMMEDIATE/COMMIT by hand in claim.
        self._conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")  # wait for the write-lock instead of erroring
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS messages "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, event TEXT NOT NULL, "
            "locked_by TEXT, lock_expiry REAL)"
        )
        self._clock = clock

    def publish(self, group_id: str, event: Event) -> None:
        self._conn.execute(
            "INSERT INTO messages (group_id, event) VALUES (?, ?)",
            (group_id, event.model_dump_json()),
        )

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT seq, group_id, event FROM messages m "
                "WHERE (m.locked_by IS NULL OR m.lock_expiry < ?) "
                "AND m.group_id NOT IN ("
                "  SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?"
                ") ORDER BY m.seq LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                self._conn.execute("COMMIT")
                return None
            seq, group_id, event = row
            self._conn.execute(
                "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (worker_id, now + visibility, seq),
            )
            self._conn.execute("COMMIT")
            return Lease(seq, group_id, Event.model_validate_json(event))
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def ack(self, lease: Lease) -> None:
        self._conn.execute("DELETE FROM messages WHERE seq = ?", (lease.seq,))

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            self._conn.execute(
                "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (_PARKED, self._clock() + delay, lease.seq),
            )
        else:
            self._conn.execute(
                "UPDATE messages SET locked_by = NULL, lock_expiry = NULL WHERE seq = ?",
                (lease.seq,),
            )

    def close(self) -> None:
        self._conn.close()


class RedisTransport:
    """`Transport` over Redis, with the per-group exclusivity built by hand since
    Redis has no native message groups:

    - `q:{G}` — a list per group, the FIFO (RPUSH to enqueue, the head is oldest).
    - `lock:{G}` — `SET NX PX` is the group lock *and* the fencing token: only one
      worker holds it (the synchronous mutual exclusion that makes the claim race
      safe), and its TTL (the visibility timeout) auto-releases it if the worker
      dies, so the head becomes claimable again.
    - `ready` — a sorted set of groups that have messages, scored by the epoch-ms
      at which the group is next claimable (0 = now). `claim` reads only the few
      lowest-scored due groups (`ZRANGEBYSCORE -inf now LIMIT 0 K`), so its cost is
      O(log N + K) in the number of pending groups — NOT a full scan of every group
      (the old `SMEMBERS groups`, which collapsed throughput under a large backlog).
      Leasing a group bumps its score to `now + visibility`, so other claimers skip
      it AND it reappears on its own once the lease expires (the expiry-recovery
      timer, with no separate sweep).

    `claim` locks a due group and returns its head without removing it; `ack` (lock
    still owned) pops the head and re-readies the group (or drops it); `nack`
    re-readies now, or parks it for `delay`. The client is injected (any redis-py-
    compatible client, e.g. fakeredis), so `redis` is an optional dependency. NOTE:
    like the sqlite lease, a lock that expires mid-ack can let two workers touch one
    group; the store's version/CAS is the backstop (a stale worker's commit is
    rejected)."""

    # how many lowest-scored due groups `claim` considers per call — bounds the work
    # so it never scales with the total number of pending groups (a contended head
    # group does not starve other ready groups).
    _CANDIDATES = 8

    def __init__(self, client: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._r = client
        self._prefix = prefix
        self._clock = clock  # injectable so the ready-score clock is deterministic in tests

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "RedisTransport":
        """Convenience constructor; imports `redis` lazily (the optional dep)."""
        import redis

        return cls(redis.Redis.from_url(url), prefix)

    def _k_ready(self) -> str:
        return f"{self._prefix}:ready"

    def _k_q(self, group_id: str) -> str:
        return f"{self._prefix}:q:{group_id}"

    def _k_lock(self, group_id: str) -> str:
        return f"{self._prefix}:lock:{group_id}"

    @staticmethod
    def _decode(value: Any) -> Optional[str]:
        if value is None:
            return None
        return value.decode() if isinstance(value, (bytes, bytearray)) else value

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    def publish(self, group_id: str, event: Event) -> None:
        pipe = self._r.pipeline()
        pipe.rpush(self._k_q(group_id), event.model_dump_json())
        # NX: never reset the score of a group that is already scheduled — a publish
        # into an in-flight or parked group must not make it claimable before its
        # lease/park elapses. A brand-new group gets score 0 (claimable now).
        pipe.zadd(self._k_ready(), {group_id: 0}, nx=True)
        pipe.execute()

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        px = max(1, int(visibility * 1000))
        now = self._now_ms()
        # only the few lowest-scored groups that are due now — O(log N + K), not O(N)
        candidates = self._r.zrangebyscore(self._k_ready(), "-inf", now, start=0, num=self._CANDIDATES)
        for raw in candidates:
            group_id = self._decode(raw)
            assert group_id is not None
            token = f"{worker_id}:{uuid.uuid4().hex}"
            # SET NX PX is the per-group lock: only one worker wins the race for a
            # candidate, and it expires on its own (the lease) if the worker dies.
            if not self._r.set(self._k_lock(group_id), token, nx=True, px=px):
                continue  # held by another worker -> try the next candidate
            payload = self._decode(self._r.lindex(self._k_q(group_id), 0))
            if payload is None:
                # a stale group with no messages: drop it and release the lock
                self._r.zrem(self._k_ready(), group_id)
                self._r.delete(self._k_lock(group_id))
                continue
            # bump the score out by the visibility window: concurrent claimers skip
            # it, and it reappears as a candidate once the lease expires (recovery).
            self._r.zadd(self._k_ready(), {group_id: now + px})
            return Lease(seq=0, group_id=group_id, event=Event.model_validate_json(payload), token=token)
        return None

    def _owns(self, group_id: str, token: str) -> bool:
        return self._decode(self._r.get(self._k_lock(group_id))) == token

    def ack(self, lease: Lease) -> None:
        # fencing: only the current lock holder removes the head + frees the group
        if not self._owns(lease.group_id, lease.token):
            return
        self._r.lpop(self._k_q(lease.group_id))
        if self._r.llen(self._k_q(lease.group_id)) == 0:
            self._r.zrem(self._k_ready(), lease.group_id)
        else:
            self._r.zadd(self._k_ready(), {lease.group_id: 0})  # next message claimable now (FIFO)
        self._r.delete(self._k_lock(lease.group_id))

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            # park: not claimable until `delay` passes (score in the future), and keep
            # the lock for the same window so the still-present head isn't re-claimed.
            self._r.zadd(self._k_ready(), {lease.group_id: self._now_ms() + int(delay * 1000)})
            self._r.set(self._k_lock(lease.group_id), lease.token, px=max(1, int(delay * 1000)))
        else:
            # release: re-ready now and drop the lock so the head can be re-claimed
            self._r.zadd(self._k_ready(), {lease.group_id: 0})
            self._r.delete(self._k_lock(lease.group_id))

    def close(self) -> None:
        self._r.close()


class PostgresTransport:
    """`Transport` over PostgreSQL — a multi-machine queue with no Redis (the classic
    DB-as-queue). `transport_messages` is the FIFO; per-group exclusivity is a **per-group
    row** in `transport_groups` carrying the lease (`locked_by` token + `lock_expiry`).

    `claim` leases a claimable group with **`SELECT … FOR UPDATE SKIP LOCKED`**: Postgres's
    row lock makes the per-group selection race-free, and SKIP LOCKED lets concurrent workers
    lease *different* groups in parallel — so claims do not serialize. (The previous design
    took a single global `pg_advisory_xact_lock` to serialize every claim, which made the
    whole transport a bottleneck — one claim at a time regardless of worker count. This is the
    same per-group + SKIP LOCKED approach DBOS uses for its Postgres queue.) Lease times are
    the client clock (epoch float). A claimed group's head message is returned but not removed;
    `ack` removes it and frees the group (fenced by the lease token); `nack` frees it now or
    parks it for `delay`.

    The connection is injected (duck-typed), so `psycopg` is an optional extra. `prefix` is
    accepted for API compatibility (the table names are fixed)."""

    def __init__(self, conn: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._conn = conn
        self._clock = clock
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS transport_messages "
                "(seq BIGSERIAL PRIMARY KEY, group_id TEXT NOT NULL, event TEXT NOT NULL)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS transport_messages_group ON transport_messages (group_id, seq)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS transport_groups "
                "(group_id TEXT PRIMARY KEY, locked_by TEXT, lock_expiry DOUBLE PRECISION)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS transport_groups_claimable ON transport_groups (lock_expiry)"
            )
        conn.commit()

    @classmethod
    def from_dsn(cls, dsn: str, connect_retries: int = 15, retry_delay: float = 1.0) -> "PostgresTransport":
        import time as _time

        import psycopg

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(psycopg.connect(dsn))
            except psycopg.OperationalError as exc:
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("postgres connect failed")

    def publish(self, group_id: str, event: Event) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)",
                (group_id, event.model_dump_json()),
            )
            # ready the group iff new (ON CONFLICT DO NOTHING) — a publish into an in-flight or
            # parked group must not reset its lease
            cur.execute(
                "INSERT INTO transport_groups (group_id, locked_by, lock_expiry) VALUES (%s, NULL, NULL) "
                "ON CONFLICT (group_id) DO NOTHING",
                (group_id,),
            )
        self._conn.commit()

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        try:
            with self._conn.cursor() as cur:
                while True:
                    token = f"{worker_id}:{uuid.uuid4().hex}"
                    # lease one claimable group; FOR UPDATE SKIP LOCKED locks that group row so a
                    # concurrent claimer skips it and takes a different group (parallel, no global lock)
                    cur.execute(
                        "UPDATE transport_groups SET locked_by = %s, lock_expiry = %s WHERE group_id = ("
                        "  SELECT group_id FROM transport_groups "
                        "  WHERE locked_by IS NULL OR lock_expiry < %s "
                        "  ORDER BY group_id FOR UPDATE SKIP LOCKED LIMIT 1"
                        ") RETURNING group_id",
                        (token, now + visibility, now),
                    )
                    grow = cur.fetchone()
                    if grow is None:
                        self._conn.commit()
                        return None
                    group_id = grow[0]
                    cur.execute(
                        "SELECT seq, event FROM transport_messages WHERE group_id = %s ORDER BY seq LIMIT 1",
                        (group_id,),
                    )
                    head = cur.fetchone()
                    if head is None:
                        # drained group (last message already acked): drop its row and keep looking
                        cur.execute(
                            "DELETE FROM transport_groups WHERE group_id = %s AND locked_by = %s",
                            (group_id, token),
                        )
                        continue
                    self._conn.commit()
                    return Lease(head[0], group_id, Event.model_validate_json(head[1]), token=token)
        except Exception:
            self._conn.rollback()
            raise

    def ack(self, lease: Lease) -> None:
        with self._conn.cursor() as cur:
            # fence: only the current lease holder removes the head + frees the group
            cur.execute(
                "SELECT 1 FROM transport_groups WHERE group_id = %s AND locked_by = %s",
                (lease.group_id, lease.token),
            )
            if cur.fetchone() is not None:
                cur.execute("DELETE FROM transport_messages WHERE seq = %s", (lease.seq,))
                cur.execute(
                    "UPDATE transport_groups SET locked_by = NULL, lock_expiry = NULL "
                    "WHERE group_id = %s AND locked_by = %s",
                    (lease.group_id, lease.token),
                )
        self._conn.commit()

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        with self._conn.cursor() as cur:
            if delay > 0:
                # park: keep the token so the still-present head isn't re-claimed until `delay` passes
                cur.execute(
                    "UPDATE transport_groups SET lock_expiry = %s WHERE group_id = %s AND locked_by = %s",
                    (self._clock() + delay, lease.group_id, lease.token),
                )
            else:
                cur.execute(
                    "UPDATE transport_groups SET locked_by = NULL, lock_expiry = NULL "
                    "WHERE group_id = %s AND locked_by = %s",
                    (lease.group_id, lease.token),
                )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class RqliteTransport:
    """`Transport` over rqlite — a multi-machine queue on distributed SQLite. rqlite
    serializes all writes through the Raft leader, so the per-group exclusivity
    selection is race-free in a single statement (like SQLite's write-lock). `claim`
    leases the oldest deliverable message with a unique token in one UPDATE, then
    reads that row back by token. Lease times are the client clock. The base URL is
    injected, so `requests` is an optional extra."""

    def __init__(self, base_url: str, timeout: float = 10.0, clock: Callable[[], float] = time.time) -> None:
        import requests

        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._clock = clock
        self._session = requests.Session()
        self._execute(
            [
                "CREATE TABLE IF NOT EXISTS messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                "group_id TEXT NOT NULL, event TEXT NOT NULL, locked_by TEXT, lock_expiry REAL)"
            ]
        )

    @classmethod
    def from_url(cls, url: str, connect_retries: int = 30, retry_delay: float = 1.0) -> "RqliteTransport":
        import time as _time

        import requests

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(url)
            except requests.exceptions.RequestException as exc:
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("rqlite connect failed")

    def _execute(self, statements: list) -> list:
        resp = self._session.post(f"{self._base}/db/execute", json=statements, timeout=self._timeout)
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

    def publish(self, group_id: str, event: Event) -> None:
        self._execute(
            [["INSERT INTO messages (group_id, event) VALUES (?, ?)", group_id, event.model_dump_json()]]
        )

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        # one serialized UPDATE leases the oldest deliverable message with our token
        results = self._execute(
            [
                [
                    "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ("
                    "  SELECT seq FROM messages m WHERE (m.locked_by IS NULL OR m.lock_expiry < ?) "
                    "    AND m.group_id NOT IN ("
                    "      SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?"
                    "    ) ORDER BY m.seq LIMIT 1)",
                    token,
                    now + visibility,
                    now,
                    now,
                ]
            ]
        )
        if results[0].get("rows_affected", 0) == 0:
            return None
        rows = self._query("SELECT seq, group_id, event FROM messages WHERE locked_by = ?", (token,))
        seq, group_id, event = rows[0]
        return Lease(seq, group_id, Event.model_validate_json(event), token=token)

    def ack(self, lease: Lease) -> None:
        self._execute([["DELETE FROM messages WHERE seq = ?", lease.seq]])

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            self._execute(
                [
                    [
                        "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                        _PARKED,
                        self._clock() + delay,
                        lease.seq,
                    ]
                ]
            )
        else:
            self._execute(
                [["UPDATE messages SET locked_by = NULL, lock_expiry = 0 WHERE seq = ?", lease.seq]]
            )

    def close(self) -> None:
        self._session.close()


class SqsTransport:
    """`Transport` over AWS SQS **FIFO** — the native fit: SQS's `MessageGroupId`
    *is* the per-group exclusivity (no other message of a group is delivered while
    one is in-flight) and the receive **visibility timeout** *is* the lease. Works
    against real SQS or **LocalStack** (no AWS account) — just point `endpoint_url`
    at it. `boto3` is an optional extra; the client is injected.

    publish = send_message(MessageGroupId, MessageDeduplicationId=uuid); claim =
    receive_message(VisibilityTimeout) → the ReceiptHandle is the lease (`token`);
    ack = delete_message; nack = change_message_visibility(0)."""

    def __init__(self, client: Any, queue_url: str, wait_seconds: int = 1) -> None:
        self._sqs = client
        self._queue_url = queue_url
        self._wait = wait_seconds

    @classmethod
    def create(
        cls,
        endpoint_url: str,
        queue_name: str = "stm.fifo",
        region: str = "us-east-1",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "SqsTransport":
        """Build a client (LocalStack-friendly: dummy creds, injected endpoint) and
        ensure the FIFO queue exists, retrying until the endpoint is reachable."""
        import time as _time

        import boto3
        from botocore.exceptions import BotoCoreError, ClientError

        client = boto3.client(
            "sqs",
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        if not queue_name.endswith(".fifo"):
            queue_name += ".fifo"
        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                resp = client.create_queue(QueueName=queue_name, Attributes={"FifoQueue": "true"})
                return cls(client, resp["QueueUrl"])
            except (BotoCoreError, ClientError) as exc:
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("sqs connect failed")

    def publish(self, group_id: str, event: Event) -> None:
        self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=event.model_dump_json(),
            MessageGroupId=group_id,
            MessageDeduplicationId=uuid.uuid4().hex,  # unique per send (fan-out reuses event ids)
        )

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        resp = self._sqs.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=int(visibility),
            WaitTimeSeconds=self._wait,
            AttributeNames=["MessageGroupId"],
        )
        messages = resp.get("Messages") or []
        if not messages:
            return None
        msg = messages[0]
        group_id = msg["Attributes"]["MessageGroupId"]
        return Lease(0, group_id, Event.model_validate_json(msg["Body"]), token=msg["ReceiptHandle"])

    def ack(self, lease: Lease) -> None:
        self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=lease.token)

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        # SQS's native park: hide the message for `delay` seconds (0 = available now)
        self._sqs.change_message_visibility(
            QueueUrl=self._queue_url, ReceiptHandle=lease.token, VisibilityTimeout=int(delay)
        )

    def close(self) -> None:
        self._sqs.close()


class MongoTransport:
    """`Transport` over MongoDB — a multi-machine queue (the document-store
    sibling of the SQL queues), no Redis. MongoDB has no native message groups,
    so — like `RedisTransport` — the per-group exclusivity is built by hand:

    - ``{prefix}_messages`` — the FIFO, one document per message keyed by a
      monotonic `_id` seq (oldest = smallest seq).
    - ``{prefix}_locks`` — one document per group that has messages, the
      **ready-index + lock in one**: `available_at` is the epoch at which the
      group is next claimable (0 = now), and `token` is the current lease (for
      fencing). `claim` reads only the few lowest `available_at <= now` groups
      (`find(...).sort(available_at).limit(K)`), so its cost is O(log N + K) in the
      number of *active groups* — NOT a `$group` aggregation over every message (the
      old design, which scanned the whole `messages` collection on each claim and
      collapsed under a backlog). Leasing bumps `available_at` to `now + visibility`,
      so concurrent claimers skip the group AND it reappears on its own once the
      lease expires (crash recovery, no separate sweep).

    `claim` atomically leases the group (a `find_one_and_update` whose filter still
    requires `available_at <= now`, so only one worker wins the race) and returns its
    head without removing it; `ack` (lock still owned) deletes the head and re-readies
    the group (`available_at=0`) or drops it if empty; `nack` re-readies now, or parks
    it for `delay`. The client is injected (duck-typed), so `pymongo` is an optional
    extra and tests use mongomock. NOTE: like the other lease backends, a lock that
    expires mid-ack can let two workers touch one group; the store's version/CAS is the
    backstop."""

    # how many lowest-`available_at` due groups `claim` considers per call — bounds the
    # work so it never scales with the total number of active groups.
    _CANDIDATES = 8

    def __init__(
        self, client: Any, db_name: str = "harel", prefix: str = "stm", clock: Callable[[], float] = time.time
    ) -> None:
        from pymongo import ReturnDocument

        self._client = client
        self._db = client[db_name]
        self._msgs = self._db[f"{prefix}_messages"]
        self._locks = self._db[f"{prefix}_locks"]
        self._counters = self._db[f"{prefix}_counters"]
        self._after = ReturnDocument.AFTER
        self._clock = clock

    @classmethod
    def from_url(
        cls, url: str, db_name: str = "harel", connect_retries: int = 30, retry_delay: float = 1.0
    ) -> "MongoTransport":
        import time as _time

        import pymongo
        from pymongo.errors import PyMongoError

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                client: Any = pymongo.MongoClient(url)
                client.admin.command("ping")
                inst = cls(client, db_name)
                inst._locks.create_index("available_at")  # the claim index (O(log N + K))
                return inst
            except PyMongoError as exc:
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("mongo connect failed")

    def _next_seq(self) -> int:
        doc = self._counters.find_one_and_update(
            {"_id": "seq"}, {"$inc": {"n": 1}}, upsert=True, return_document=self._after
        )
        return int(doc["n"])

    def publish(self, group_id: str, event: Event) -> None:
        self._msgs.insert_one(
            {"_id": self._next_seq(), "group_id": group_id, "event": event.model_dump_json()}
        )
        # ready the group NOW iff it is new ($setOnInsert): a publish into an in-flight or
        # parked group must not make it claimable before its lease/park elapses.
        self._locks.update_one(
            {"_id": group_id}, {"$setOnInsert": {"available_at": 0.0, "token": None}}, upsert=True
        )

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        # only the few lowest-`available_at` groups due now — O(log N + K), not a scan
        candidates = (
            self._locks.find({"available_at": {"$lte": now}}).sort("available_at", 1).limit(self._CANDIDATES)
        )
        for c in list(candidates):
            group_id = c["_id"]
            token = f"{worker_id}:{uuid.uuid4().hex}"
            # atomic lease: re-check `available_at <= now` in the filter so only one worker
            # wins; the loser's filter no longer matches once the winner bumps it out.
            leased = self._locks.find_one_and_update(
                {"_id": group_id, "available_at": {"$lte": now}},
                {"$set": {"token": token, "available_at": now + visibility}},
            )
            if leased is None:
                continue  # another worker leased it first
            head = self._msgs.find_one({"group_id": group_id}, sort=[("_id", 1)])
            if head is None:
                self._locks.delete_one({"_id": group_id, "token": token})  # stale group, release
                continue
            return Lease(head["_id"], group_id, Event.model_validate_json(head["event"]), token=token)
        return None

    def _owns(self, group_id: str, token: str) -> bool:
        doc = self._locks.find_one({"_id": group_id})
        return doc is not None and doc.get("token") == token

    def ack(self, lease: Lease) -> None:
        if not self._owns(lease.group_id, lease.token):
            return  # fencing: only the current lock holder removes + re-readies
        self._msgs.delete_one({"_id": lease.seq})
        if self._msgs.find_one({"group_id": lease.group_id}) is not None:
            # more messages: claimable now, in FIFO order (next head)
            self._locks.update_one(
                {"_id": lease.group_id, "token": lease.token},
                {"$set": {"available_at": 0.0, "token": None}},
            )
        else:
            self._locks.delete_one({"_id": lease.group_id, "token": lease.token})

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            # park: not claimable until `delay` passes; keep the token so the still-present
            # head isn't re-claimed before then
            self._locks.update_one(
                {"_id": lease.group_id, "token": lease.token},
                {"$set": {"available_at": self._clock() + delay}},
            )
        else:
            self._locks.update_one(
                {"_id": lease.group_id, "token": lease.token},
                {"$set": {"available_at": 0.0, "token": None}},
            )

    def close(self) -> None:
        self._client.close()


