"""Async `Transport` — the async sibling of `harel.engine.transport`.

Same contract (single-active-consumer per group, FIFO within a group, lease/visibility,
`nack(delay)` parking), every method `async def`. The sync transports stay untouched.

Holds the `AsyncTransport` Protocol + `AsyncInMemoryTransport`. The networked async
backends (sqlite/redis/postgres) are added in later phases. `Lease` is reused as-is.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Callable, Optional, Protocol, runtime_checkable

from harel.engine.transport import _PARKED, Lease
from harel.spec.states import Event


@runtime_checkable
class AsyncTransport(Protocol):
    """Async mirror of `Transport`: identical per-group-exclusivity semantics, awaited IO."""

    async def publish(self, group_id: str, event: Event) -> None: ...

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]: ...

    async def ack(self, lease: Lease) -> None: ...

    async def nack(self, lease: Lease, delay: float = 0.0) -> None: ...

    async def close(self) -> None: ...


class AsyncInMemoryTransport:
    """Same-process async `Transport`: a faithful async mirror of `InMemoryTransport`
    (lease/visibility via `lock_expiry`, `_PARKED` parking for `nack(delay)`). No lock —
    a single event loop serializes the (await-free) critical sections, doing what the
    sync transport's `threading.Lock` does across threads."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._messages: list[dict] = []
        self._seq = 0
        self._clock = clock

    async def publish(self, group_id: str, event: Event) -> None:
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

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        in_flight = {
            m["group_id"] for m in self._messages if m["locked_by"] is not None and m["lock_expiry"] >= now
        }
        for m in sorted(self._messages, key=lambda m: m["seq"]):
            available = m["locked_by"] is None or m["lock_expiry"] < now
            if available and m["group_id"] not in in_flight:
                m["locked_by"] = worker_id
                m["lock_expiry"] = now + visibility
                return Lease(m["seq"], m["group_id"], m["event"])
        return None

    async def ack(self, lease: Lease) -> None:
        self._messages = [m for m in self._messages if m["seq"] != lease.seq]

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        for m in self._messages:
            if m["seq"] == lease.seq:
                if delay > 0:
                    m["locked_by"] = _PARKED
                    m["lock_expiry"] = self._clock() + delay
                else:
                    m["locked_by"] = None
                    m["lock_expiry"] = 0.0

    async def close(self) -> None:
        pass


class AsyncSqliteTransport:
    """Async mirror of `SqliteTransport` over `aiosqlite`. `claim` runs inside
    `BEGIN IMMEDIATE` so SQLite's global write-lock serializes claims (race-free per-group
    exclusivity with plain SQL); the lease (`lock_expiry`) recovers a crashed worker's
    message. Build with `await AsyncSqliteTransport.create(path)`."""

    def __init__(self, conn: Any, clock: Callable[[], float] = time.time) -> None:
        self._conn = conn
        self._clock = clock

    @classmethod
    async def create(
        cls, path: str = ":memory:", clock: Callable[[], float] = time.time
    ) -> "AsyncSqliteTransport":
        import aiosqlite

        conn = await aiosqlite.connect(str(path), isolation_level=None)  # autocommit; BEGIN by hand
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA busy_timeout=5000")
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS messages "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, event TEXT NOT NULL, "
            "locked_by TEXT, lock_expiry REAL)"
        )
        return cls(conn, clock)

    async def publish(self, group_id: str, event: Event) -> None:
        await self._conn.execute(
            "INSERT INTO messages (group_id, event) VALUES (?, ?)", (group_id, event.model_dump_json())
        )

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await self._conn.execute(
                "SELECT seq, group_id, event FROM messages m "
                "WHERE (m.locked_by IS NULL OR m.lock_expiry < ?) "
                "AND m.group_id NOT IN ("
                "  SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?"
                ") ORDER BY m.seq LIMIT 1",
                (now, now),
            )
            row = await cur.fetchone()
            if row is None:
                await self._conn.execute("COMMIT")
                return None
            seq, group_id, event = row
            await self._conn.execute(
                "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (worker_id, now + visibility, seq),
            )
            await self._conn.execute("COMMIT")
            return Lease(seq, group_id, Event.model_validate_json(event))
        except Exception:
            await self._conn.execute("ROLLBACK")
            raise

    async def ack(self, lease: Lease) -> None:
        await self._conn.execute("DELETE FROM messages WHERE seq = ?", (lease.seq,))

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            await self._conn.execute(
                "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (_PARKED, self._clock() + delay, lease.seq),
            )
        else:
            await self._conn.execute(
                "UPDATE messages SET locked_by = NULL, lock_expiry = NULL WHERE seq = ?", (lease.seq,)
            )

    async def close(self) -> None:
        await self._conn.close()


class AsyncPostgresTransport:
    """Async mirror of `PostgresTransport` over `psycopg.AsyncConnection`: a queue table;
    `claim` takes a global `pg_advisory_xact_lock` (serializes claims like SQLite's write-lock)
    then leases the oldest deliverable message of a group with nothing in-flight. Build with
    `await AsyncPostgresTransport.from_dsn(dsn)`."""

    def __init__(self, conn: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._conn = conn
        self._prefix = prefix
        self._clock = clock

    @classmethod
    async def from_dsn(
        cls, dsn: str, prefix: str = "stm", clock: Callable[[], float] = time.time
    ) -> "AsyncPostgresTransport":
        import psycopg

        conn = await psycopg.AsyncConnection.connect(dsn)
        async with conn.cursor() as cur:
            await cur.execute(
                "CREATE TABLE IF NOT EXISTS transport_messages "
                "(seq BIGSERIAL PRIMARY KEY, group_id TEXT NOT NULL, event TEXT NOT NULL, "
                "locked_by TEXT, lock_expiry DOUBLE PRECISION)"
            )
        await conn.commit()
        return cls(conn, prefix, clock)

    async def publish(self, group_id: str, event: Event) -> None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)",
                (group_id, event.model_dump_json()),
            )
        await self._conn.commit()

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        try:
            async with self._conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s)::int8)", (f"{self._prefix}:claim",)
                )
                await cur.execute(
                    "UPDATE transport_messages SET locked_by = %s, lock_expiry = %s WHERE seq = ("
                    "  SELECT seq FROM transport_messages m "
                    "  WHERE (m.locked_by IS NULL OR m.lock_expiry < %s) "
                    "    AND m.group_id NOT IN ("
                    "      SELECT group_id FROM transport_messages WHERE locked_by IS NOT NULL AND lock_expiry >= %s"
                    "    ) ORDER BY m.seq LIMIT 1"
                    ") RETURNING seq, group_id, event",
                    (worker_id, now + visibility, now, now),
                )
                row = await cur.fetchone()
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise
        if row is None:
            return None
        return Lease(row[0], row[1], Event.model_validate_json(row[2]))

    async def ack(self, lease: Lease) -> None:
        async with self._conn.cursor() as cur:
            await cur.execute("DELETE FROM transport_messages WHERE seq = %s", (lease.seq,))
        await self._conn.commit()

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        async with self._conn.cursor() as cur:
            if delay > 0:
                await cur.execute(
                    "UPDATE transport_messages SET locked_by = %s, lock_expiry = %s WHERE seq = %s",
                    (_PARKED, self._clock() + delay, lease.seq),
                )
            else:
                await cur.execute(
                    "UPDATE transport_messages SET locked_by = NULL, lock_expiry = NULL WHERE seq = %s",
                    (lease.seq,),
                )
        await self._conn.commit()

    async def close(self) -> None:
        await self._conn.close()


class AsyncRedisTransport:
    """Async mirror of `RedisTransport` over `redis.asyncio`: per-group exclusivity by hand
    (`SET NX PX` group-lock-as-lease + a list per group), and a `ready` ZSET scored by
    available-at time so `claim` reads only the few lowest-scored due groups (O(log N + K),
    not a full scan). Leasing bumps the score (concurrent claimers skip it + free expiry
    recovery). The client is injected (fakeredis.aioredis in tests)."""

    _CANDIDATES = 8

    def __init__(self, client: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._r = client
        self._prefix = prefix
        self._clock = clock

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "AsyncRedisTransport":
        import redis.asyncio as aioredis

        return cls(aioredis.Redis.from_url(url), prefix)

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

    async def publish(self, group_id: str, event: Event) -> None:
        async with self._r.pipeline() as pipe:
            pipe.rpush(self._k_q(group_id), event.model_dump_json())
            pipe.zadd(self._k_ready(), {group_id: 0}, nx=True)
            await pipe.execute()

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        px = max(1, int(visibility * 1000))
        now = self._now_ms()
        candidates = await self._r.zrangebyscore(self._k_ready(), "-inf", now, start=0, num=self._CANDIDATES)
        for raw in candidates:
            group_id = self._decode(raw)
            assert group_id is not None
            token = f"{worker_id}:{uuid.uuid4().hex}"
            if not await self._r.set(self._k_lock(group_id), token, nx=True, px=px):
                continue
            payload = self._decode(await self._r.lindex(self._k_q(group_id), 0))
            if payload is None:
                await self._r.zrem(self._k_ready(), group_id)
                await self._r.delete(self._k_lock(group_id))
                continue
            await self._r.zadd(self._k_ready(), {group_id: now + px})
            return Lease(seq=0, group_id=group_id, event=Event.model_validate_json(payload), token=token)
        return None

    async def _owns(self, group_id: str, token: str) -> bool:
        return self._decode(await self._r.get(self._k_lock(group_id))) == token

    async def ack(self, lease: Lease) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        await self._r.lpop(self._k_q(lease.group_id))
        if await self._r.llen(self._k_q(lease.group_id)) == 0:
            await self._r.zrem(self._k_ready(), lease.group_id)
        else:
            await self._r.zadd(self._k_ready(), {lease.group_id: 0})
        await self._r.delete(self._k_lock(lease.group_id))

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            await self._r.zadd(self._k_ready(), {lease.group_id: self._now_ms() + int(delay * 1000)})
            await self._r.set(self._k_lock(lease.group_id), lease.token, px=max(1, int(delay * 1000)))
        else:
            await self._r.zadd(self._k_ready(), {lease.group_id: 0})
            await self._r.delete(self._k_lock(lease.group_id))

    async def close(self) -> None:
        await self._r.aclose()


class AsyncSurrealTransport:
    """Async mirror of `SurrealTransport`: per-group exclusivity via a `THROW`-gated
    `BEGIN … COMMIT` lock-acquire block (awaited), FIFO via a `messages` table,
    `lock_expiry` lease for crash recovery. The client is injected (an already-connected
    `AsyncSurreal`), so tests use the in-process `mem://` engine."""

    def __init__(self, client: Any, clock: Callable[[], float] = time.time) -> None:
        from surrealdb import SurrealError

        self._db = client
        self._SurrealError = SurrealError
        self._clock = clock

    @classmethod
    async def from_url(
        cls,
        url: str,
        namespace: str = "harel",
        database: str = "harel",
        username: Optional[str] = None,
        password: Optional[str] = None,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncSurrealTransport":
        import anyio
        from surrealdb import AsyncSurreal

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                client: Any = AsyncSurreal(url)
                await client.connect()
                if username is not None:
                    await client.signin({"username": username, "password": password})
                await client.use(namespace, database)
                await client.query("INFO FOR DB")
                return cls(client)
            except Exception as exc:  # noqa: BLE001
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("surreal connect failed")

    async def _next_seq(self) -> int:
        res = await self._db.query("UPSERT counter:msg SET v = (v ?? 0) + 1 RETURN v")
        return int(res[0]["v"])

    async def publish(self, group_id: str, event: Event) -> None:
        await self._db.query(
            "CREATE messages SET seq=$s, group_id=$g, event=$e",
            {"s": await self._next_seq(), "g": group_id, "e": event.model_dump_json()},
        )

    _ACQUIRE = (
        "BEGIN;\n"
        "LET $l = (SELECT id FROM type::thing('locks',$g) WHERE lock_expiry > $now);\n"
        "IF array::len($l) > 0 { THROW 'held' };\n"
        "UPSERT type::thing('locks',$g) SET token=$tok, lock_expiry=$exp;\n"
        "COMMIT;"
    )

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        groups = await self._db.query(
            "SELECT group_id, math::min(seq) AS head FROM messages GROUP BY group_id ORDER BY head ASC"
        )
        for g in groups:
            group_id = g["group_id"]
            token = f"{worker_id}:{uuid.uuid4().hex}"
            try:
                await self._db.query(
                    self._ACQUIRE,
                    {"g": group_id, "now": now, "tok": token, "exp": now + visibility},
                )
            except self._SurrealError:
                continue  # held by another worker
            head = await self._db.query(
                "SELECT seq, event FROM messages WHERE group_id=$g ORDER BY seq ASC LIMIT 1",
                {"g": group_id},
            )
            if not head:
                await self._db.query("DELETE type::thing('locks',$g)", {"g": group_id})
                continue
            row = head[0]
            return Lease(row["seq"], group_id, Event.model_validate_json(row["event"]), token=token)
        return None

    async def _owns(self, group_id: str, token: str) -> bool:
        res = await self._db.query(
            "SELECT token FROM type::thing('locks',$g)", {"g": group_id}
        )
        return bool(res) and res[0].get("token") == token

    async def ack(self, lease: Lease) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        await self._db.query("DELETE messages WHERE seq=$s", {"s": lease.seq})
        await self._db.query("DELETE type::thing('locks',$g)", {"g": lease.group_id})

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            await self._db.query(
                "UPDATE type::thing('locks',$g) SET lock_expiry=$exp",
                {"g": lease.group_id, "exp": self._clock() + delay},
            )
        else:
            await self._db.query("DELETE type::thing('locks',$g)", {"g": lease.group_id})

    async def close(self) -> None:
        await self._db.close()


class AsyncSqsTransport:
    """Async mirror of `SqsTransport`: each boto3 SQS call runs in `asyncio.to_thread`
    so it does not block the event loop. SQS FIFO semantics (per-group exclusivity via
    `MessageGroupId`, `ReceiptHandle` as the lease) are unchanged. boto3 clients are
    thread-safe; moto's `mock_aws` works in threads — tests need no server.
    Build with `await AsyncSqsTransport.create(endpoint_url, ...)` or inject a client."""

    def __init__(self, client: Any, queue_url: str, wait_seconds: int = 1) -> None:
        from harel.engine.transport import SqsTransport

        self._sync = SqsTransport(client, queue_url, wait_seconds)

    @classmethod
    async def create(
        cls,
        endpoint_url: str,
        queue_name: str = "stm.fifo",
        region: str = "us-east-1",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncSqsTransport":
        from harel.engine.transport import SqsTransport

        sync = await asyncio.to_thread(
            SqsTransport.create,
            endpoint_url=endpoint_url,
            queue_name=queue_name,
            region=region,
            connect_retries=connect_retries,
            retry_delay=retry_delay,
        )
        inst = cls.__new__(cls)
        inst._sync = sync
        return inst

    async def publish(self, group_id: str, event: Event) -> None:
        await asyncio.to_thread(self._sync.publish, group_id, event)

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        return await asyncio.to_thread(self._sync.claim, worker_id, visibility)

    async def ack(self, lease: Lease) -> None:
        await asyncio.to_thread(self._sync.ack, lease)

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        await asyncio.to_thread(self._sync.nack, lease, delay)

    async def close(self) -> None:
        await asyncio.to_thread(self._sync.close)
