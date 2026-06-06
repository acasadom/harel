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
    - `lock:{G}` — `SET NX PX` is the group lock *and* the lease: only one worker
      holds it, and its TTL (the visibility timeout) auto-releases it if the
      worker dies, so the head message becomes claimable again.
    - `groups` — the set of groups that currently have messages, to claim over.

    `claim` acquires the first unlocked group's lock and returns its head without
    removing it; `ack` (lock still owned) pops the head and releases; `nack` just
    releases. The client is injected (any redis-py-compatible client, e.g.
    fakeredis), so `redis` is an optional dependency. NOTE: like the sqlite lease,
    a lock that expires mid-ack can let two workers touch one group; the store's
    version/CAS is the backstop (a stale worker's commit is rejected)."""

    def __init__(self, client: Any, prefix: str = "stm") -> None:
        self._r = client
        self._prefix = prefix

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "RedisTransport":
        """Convenience constructor; imports `redis` lazily (the optional dep)."""
        import redis

        return cls(redis.Redis.from_url(url), prefix)

    def _k_groups(self) -> str:
        return f"{self._prefix}:groups"

    def _k_q(self, group_id: str) -> str:
        return f"{self._prefix}:q:{group_id}"

    def _k_lock(self, group_id: str) -> str:
        return f"{self._prefix}:lock:{group_id}"

    @staticmethod
    def _decode(value: Any) -> Optional[str]:
        if value is None:
            return None
        return value.decode() if isinstance(value, (bytes, bytearray)) else value

    def publish(self, group_id: str, event: Event) -> None:
        pipe = self._r.pipeline()
        pipe.rpush(self._k_q(group_id), event.model_dump_json())
        pipe.sadd(self._k_groups(), group_id)
        pipe.execute()

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        px = max(1, int(visibility * 1000))
        for raw in self._r.smembers(self._k_groups()):
            group_id = self._decode(raw)
            assert group_id is not None
            token = f"{worker_id}:{uuid.uuid4().hex}"
            # SET NX PX is the per-group lock: only one worker wins, and it expires
            # on its own (the lease) if the worker dies.
            if not self._r.set(self._k_lock(group_id), token, nx=True, px=px):
                continue  # held by another worker
            payload = self._decode(self._r.lindex(self._k_q(group_id), 0))
            if payload is None:
                # a stale group with no messages: drop it and release the lock
                self._r.srem(self._k_groups(), group_id)
                self._r.delete(self._k_lock(group_id))
                continue
            return Lease(seq=0, group_id=group_id, event=Event.model_validate_json(payload), token=token)
        return None

    def _owns(self, group_id: str, token: str) -> bool:
        return self._decode(self._r.get(self._k_lock(group_id))) == token

    def ack(self, lease: Lease) -> None:
        # fencing: only the current lock holder removes the head + releases the group
        if not self._owns(lease.group_id, lease.token):
            return
        self._r.lpop(self._k_q(lease.group_id))
        if self._r.llen(self._k_q(lease.group_id)) == 0:
            self._r.srem(self._k_groups(), lease.group_id)
        self._r.delete(self._k_lock(lease.group_id))

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            # keep the group locked for `delay` (park): claim's SET NX keeps failing
            # until the lock's TTL expires, so the still-present head isn't re-claimed.
            self._r.set(self._k_lock(lease.group_id), lease.token, px=max(1, int(delay * 1000)))
        else:
            # release the lock so the (still-present) head can be re-claimed
            self._r.delete(self._k_lock(lease.group_id))

    def close(self) -> None:
        self._r.close()


class PostgresTransport:
    """`Transport` over PostgreSQL — a multi-machine queue with no Redis (the
    classic DB-as-queue). A `transport_messages` table holds the FIFO; `claim`
    leases the oldest message of a group that has nothing in-flight. Postgres has
    real concurrency, so to get per-group exclusivity without the `SKIP LOCKED`
    same-group race, `claim` takes a single **global advisory lock** first
    (`pg_advisory_xact_lock`) — serializing claims exactly like SQLite's write-lock
    (claims are sub-ms; the real work runs after `ack`). Lease times are the
    client clock (epoch float), consistent across machines via the column.

    The connection is injected (duck-typed), so `psycopg` is an optional extra."""

    def __init__(self, conn: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._conn = conn
        self._prefix = prefix
        self._clock = clock
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS transport_messages "
                "(seq BIGSERIAL PRIMARY KEY, group_id TEXT NOT NULL, event TEXT NOT NULL, "
                "locked_by TEXT, lock_expiry DOUBLE PRECISION)"
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
        self._conn.commit()

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        try:
            with self._conn.cursor() as cur:
                # serialize claims (released at commit) so the per-group exclusivity
                # check below is race-free, like SQLite's global write-lock
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s)::int8)", (f"{self._prefix}:claim",))
                cur.execute(
                    "UPDATE transport_messages SET locked_by = %s, lock_expiry = %s WHERE seq = ("
                    "  SELECT seq FROM transport_messages m "
                    "  WHERE (m.locked_by IS NULL OR m.lock_expiry < %s) "
                    "    AND m.group_id NOT IN ("
                    "      SELECT group_id FROM transport_messages WHERE locked_by IS NOT NULL AND lock_expiry >= %s"
                    "    ) ORDER BY m.seq LIMIT 1"
                    ") RETURNING seq, group_id, event",
                    (worker_id, now + visibility, now, now),
                )
                row = cur.fetchone()
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        if row is None:
            return None
        return Lease(row[0], row[1], Event.model_validate_json(row[2]))

    def ack(self, lease: Lease) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM transport_messages WHERE seq = %s", (lease.seq,))
        self._conn.commit()

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        with self._conn.cursor() as cur:
            if delay > 0:
                cur.execute(
                    "UPDATE transport_messages SET locked_by = %s, lock_expiry = %s WHERE seq = %s",
                    (_PARKED, self._clock() + delay, lease.seq),
                )
            else:
                cur.execute(
                    "UPDATE transport_messages SET locked_by = NULL, lock_expiry = NULL WHERE seq = %s",
                    (lease.seq,),
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
    so — like `RedisTransport` — the per-group exclusivity is built by hand with a
    **per-group lock document**:

    - ``{prefix}_messages`` — the FIFO, one document per message keyed by a
      monotonic `_id` seq (oldest = smallest seq).
    - ``{prefix}_locks`` — one document per group; held via an atomic
      `find_one_and_update(..., upsert=True)` whose filter only matches a free or
      expired lock, so a `DuplicateKeyError` on the upsert means "another worker
      holds it" (skip). `lock_expiry` is the lease — a crashed worker's lock
      expires and the group becomes claimable again.

    `claim` walks the groups ordered by their oldest message, takes the first
    group whose lock is free, and returns its head without removing it; `ack`
    (lock still owned) deletes the message + releases; `nack` releases (or, with
    `delay>0`, extends the lock = park). The client is injected (duck-typed), so
    `pymongo` is an optional extra and tests use mongomock. NOTE: like the other
    lease backends, a lock that expires mid-ack can let two workers touch one
    group; the store's version/CAS is the backstop."""

    def __init__(
        self, client: Any, db_name: str = "harel", prefix: str = "stm", clock: Callable[[], float] = time.time
    ) -> None:
        from pymongo import ReturnDocument
        from pymongo.errors import DuplicateKeyError

        self._client = client
        self._db = client[db_name]
        self._msgs = self._db[f"{prefix}_messages"]
        self._locks = self._db[f"{prefix}_locks"]
        self._counters = self._db[f"{prefix}_counters"]
        self._after = ReturnDocument.AFTER
        self._DuplicateKeyError = DuplicateKeyError
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
                return cls(client, db_name)
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

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        # groups that still have messages, ordered by their oldest message
        groups = self._msgs.aggregate(
            [{"$group": {"_id": "$group_id", "head": {"$min": "$_id"}}}, {"$sort": {"head": 1}}]
        )
        for g in groups:
            group_id = g["_id"]
            token = f"{worker_id}:{uuid.uuid4().hex}"
            try:
                # acquire iff free/expired; an existing live lock makes the upsert
                # collide on _id -> DuplicateKeyError -> held by another worker
                self._locks.find_one_and_update(
                    {
                        "_id": group_id,
                        "$or": [{"lock_expiry": {"$lte": now}}, {"lock_expiry": {"$exists": False}}],
                    },
                    {"$set": {"token": token, "lock_expiry": now + visibility}},
                    upsert=True,
                )
            except self._DuplicateKeyError:
                continue
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
            return  # fencing: only the current lock holder removes + releases
        self._msgs.delete_one({"_id": lease.seq})
        self._locks.delete_one({"_id": lease.group_id, "token": lease.token})

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            # park: keep the lock held for `delay` so the still-present head is not
            # re-claimed until it expires
            self._locks.update_one(
                {"_id": lease.group_id, "token": lease.token},
                {"$set": {"lock_expiry": self._clock() + delay}},
            )
        else:
            self._locks.delete_one({"_id": lease.group_id, "token": lease.token})

    def close(self) -> None:
        self._client.close()


class SurrealTransport:
    """`Transport` over SurrealDB — a multi-machine queue with no Redis. Like
    `MongoTransport`, the per-group exclusivity is a **per-group lock record**
    (SurrealDB has no native message groups). Acquiring the lock is one atomic
    server-side `BEGIN … COMMIT` block: it `THROW`s (aborting the txn) if the
    lock is still live, so a collision means "held by another worker" (skip);
    otherwise it upserts the lock. The lock's `lock_expiry` is the lease — a
    crashed worker's lock expires and the group becomes claimable again.

    `messages` is the FIFO (one record per message, monotonic `seq`); `claim`
    walks the groups ordered by their oldest message, takes the first whose lock
    is free, and returns its head without removing it; `ack` (lock still owned)
    deletes the message + releases; `nack` releases (or, with `delay>0`, extends
    the lock = park). The client is injected (an already-connected `Surreal`), so
    `surrealdb` is an optional extra and tests use the in-process `mem://` engine.
    NOTE: like the other lease backends, a lock that expires mid-ack can let two
    workers touch one group; the store's version/CAS is the backstop."""

    def __init__(self, client: Any, clock: Callable[[], float] = time.time) -> None:
        from surrealdb import SurrealError

        self._db = client
        self._SurrealError = SurrealError
        self._clock = clock

    @classmethod
    def from_url(
        cls,
        url: str,
        namespace: str = "harel",
        database: str = "harel",
        username: Optional[str] = None,
        password: Optional[str] = None,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "SurrealTransport":
        import time as _time

        from surrealdb import Surreal

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                client: Any = Surreal(url)
                client.connect()
                if username is not None:
                    client.signin({"username": username, "password": password})
                client.use(namespace, database)
                client.query("INFO FOR DB")
                return cls(client)
            except Exception as exc:  # noqa: BLE001 — retry any connect-time failure
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("surreal connect failed")

    def _next_seq(self) -> int:
        res = self._db.query("UPSERT counter:msg SET v = (v ?? 0) + 1 RETURN v")
        return int(res[0]["v"])

    def publish(self, group_id: str, event: Event) -> None:
        self._db.query(
            "CREATE messages SET seq=$s, group_id=$g, event=$e",
            {"s": self._next_seq(), "g": group_id, "e": event.model_dump_json()},
        )

    _ACQUIRE = (
        "BEGIN;\n"
        "LET $l = (SELECT id FROM type::thing('locks',$g) WHERE lock_expiry > $now);\n"
        "IF array::len($l) > 0 { THROW 'held' };\n"
        "UPSERT type::thing('locks',$g) SET token=$tok, lock_expiry=$exp;\n"
        "COMMIT;"
    )

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        groups = self._db.query(
            "SELECT group_id, math::min(seq) AS head FROM messages GROUP BY group_id ORDER BY head ASC"
        )
        for g in groups:
            group_id = g["group_id"]
            token = f"{worker_id}:{uuid.uuid4().hex}"
            try:
                self._db.query(
                    self._ACQUIRE, {"g": group_id, "now": now, "tok": token, "exp": now + visibility}
                )
            except self._SurrealError:
                continue  # held by another worker
            head = self._db.query(
                "SELECT seq, event FROM messages WHERE group_id=$g ORDER BY seq ASC LIMIT 1", {"g": group_id}
            )
            if not head:
                self._db.query("DELETE type::thing('locks',$g)", {"g": group_id})  # stale group, release
                continue
            row = head[0]
            return Lease(row["seq"], group_id, Event.model_validate_json(row["event"]), token=token)
        return None

    def _owns(self, group_id: str, token: str) -> bool:
        res = self._db.query("SELECT token FROM type::thing('locks',$g)", {"g": group_id})
        return bool(res) and res[0].get("token") == token

    def ack(self, lease: Lease) -> None:
        if not self._owns(lease.group_id, lease.token):
            return  # fencing: only the current lock holder removes + releases
        self._db.query("DELETE messages WHERE seq=$s", {"s": lease.seq})
        self._db.query("DELETE type::thing('locks',$g)", {"g": lease.group_id})

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            # park: keep the lock held for `delay` so the still-present head is not
            # re-claimed until it expires
            self._db.query(
                "UPDATE type::thing('locks',$g) SET lock_expiry=$exp",
                {"g": lease.group_id, "exp": self._clock() + delay},
            )
        else:
            self._db.query("DELETE type::thing('locks',$g)", {"g": lease.group_id})

    def close(self) -> None:
        self._db.close()
