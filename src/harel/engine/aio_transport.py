"""Async `Transport` — the async sibling of `harel.engine.transport`.

Same contract (single-active-consumer per group, FIFO within a group, lease/visibility,
`nack(delay)` parking), every method `async def`. The sync transports stay untouched.

Holds the `AsyncTransport` Protocol + `AsyncInMemoryTransport`. The networked async
backends (sqlite/redis/postgres) are added in later phases. `Lease` is reused as-is.
"""

from __future__ import annotations

import time
import uuid
from contextlib import AsyncExitStack
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
    """Async mirror of `PostgresTransport` over `psycopg_pool.AsyncConnectionPool`: per-group
    exclusivity is a per-group row in `transport_groups` (lease = `locked_by` token +
    `lock_expiry`), and `claim` leases a claimable group with `SELECT … FOR UPDATE SKIP LOCKED`
    so concurrent workers lease *different* groups in parallel — no global lock serializing
    claims (the old `pg_advisory_xact_lock` made the transport a one-claim-at-a-time bottleneck).
    Each method checks out a pool connection. Build with
    `await AsyncPostgresTransport.from_dsn(dsn, pool_size=N)`."""

    def __init__(self, pool: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._pool = pool
        self._clock = clock

    @classmethod
    async def from_dsn(
        cls, dsn: str, prefix: str = "stm", clock: Callable[[], float] = time.time, pool_size: int = 10
    ) -> "AsyncPostgresTransport":
        from psycopg_pool import AsyncConnectionPool

        pool = AsyncConnectionPool(conninfo=dsn, min_size=1, max_size=pool_size, open=False)
        await pool.open()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS transport_messages "
                    "(seq BIGSERIAL PRIMARY KEY, group_id TEXT NOT NULL, event TEXT NOT NULL)"
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS transport_messages_group ON transport_messages (group_id, seq)"
                )
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS transport_groups "
                    "(group_id TEXT PRIMARY KEY, locked_by TEXT, lock_expiry DOUBLE PRECISION)"
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS transport_groups_claimable ON transport_groups (lock_expiry)"
                )
            await conn.commit()
        return cls(pool, prefix, clock)

    async def publish(self, group_id: str, event: Event) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)",
                    (group_id, event.model_dump_json()),
                )
                await cur.execute(
                    "INSERT INTO transport_groups (group_id, locked_by, lock_expiry) VALUES (%s, NULL, NULL) "
                    "ON CONFLICT (group_id) DO NOTHING",
                    (group_id,),
                )
            await conn.commit()

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        async with self._pool.connection() as conn:
            try:
                async with conn.cursor() as cur:
                    while True:
                        token = f"{worker_id}:{uuid.uuid4().hex}"
                        # FOR UPDATE SKIP LOCKED: a concurrent claimer skips this group's row and
                        # leases a different group — parallel claims, no global serialization
                        await cur.execute(
                            "UPDATE transport_groups SET locked_by = %s, lock_expiry = %s WHERE group_id = ("
                            "  SELECT group_id FROM transport_groups "
                            "  WHERE locked_by IS NULL OR lock_expiry < %s "
                            "  ORDER BY group_id FOR UPDATE SKIP LOCKED LIMIT 1"
                            ") RETURNING group_id",
                            (token, now + visibility, now),
                        )
                        grow = await cur.fetchone()
                        if grow is None:
                            await conn.commit()
                            return None
                        group_id = grow[0]
                        await cur.execute(
                            "SELECT seq, event FROM transport_messages WHERE group_id = %s ORDER BY seq LIMIT 1",
                            (group_id,),
                        )
                        head = await cur.fetchone()
                        if head is None:
                            await cur.execute(
                                "DELETE FROM transport_groups WHERE group_id = %s AND locked_by = %s",
                                (group_id, token),
                            )
                            continue
                        await conn.commit()
                        return Lease(head[0], group_id, Event.model_validate_json(head[1]), token=token)
            except Exception:
                await conn.rollback()
                raise

    async def ack(self, lease: Lease) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM transport_groups WHERE group_id = %s AND locked_by = %s",
                    (lease.group_id, lease.token),
                )
                if await cur.fetchone() is not None:
                    await cur.execute("DELETE FROM transport_messages WHERE seq = %s", (lease.seq,))
                    await cur.execute(
                        "UPDATE transport_groups SET locked_by = NULL, lock_expiry = NULL "
                        "WHERE group_id = %s AND locked_by = %s",
                        (lease.group_id, lease.token),
                    )
            await conn.commit()

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                if delay > 0:
                    await cur.execute(
                        "UPDATE transport_groups SET lock_expiry = %s WHERE group_id = %s AND locked_by = %s",
                        (self._clock() + delay, lease.group_id, lease.token),
                    )
                else:
                    await cur.execute(
                        "UPDATE transport_groups SET locked_by = NULL, lock_expiry = NULL "
                        "WHERE group_id = %s AND locked_by = %s",
                        (lease.group_id, lease.token),
                    )
            await conn.commit()

    async def close(self) -> None:
        await self._pool.close()


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
    """Async mirror of `SurrealTransport`: per-group control via a `locks` record that is the
    ready-index + lock in one (`available_at` = next claimable epoch, `token` = the lease).
    `claim` reads only the few lowest `available_at <= now` groups (`SELECT … ORDER BY
    available_at LIMIT K`) and leases one with a `THROW`-gated `BEGIN … COMMIT` block — O(active
    groups), not a `GROUP BY` over every message. The client is injected (an already-connected
    `AsyncSurreal`), so tests use the in-process `mem://` engine."""

    _CANDIDATES = 8

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
                await client.query(
                    "DEFINE INDEX IF NOT EXISTS locks_avail ON TABLE locks COLUMNS available_at"
                )
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
        # ready the group NOW iff it is new (`available_at ?? 0` keeps an existing value)
        await self._db.query(
            "UPSERT type::thing('locks',$g) SET available_at = available_at ?? 0, group_id = $g",
            {"g": group_id},
        )

    # atomic lease: THROW (abort) unless the group is still due, so only one racing worker wins
    _ACQUIRE = (
        "BEGIN;\n"
        "LET $d = (SELECT id FROM type::thing('locks',$g) WHERE available_at <= $now);\n"
        "IF array::len($d) == 0 { THROW 'taken' };\n"
        "UPDATE type::thing('locks',$g) SET token=$tok, available_at=$exp;\n"
        "COMMIT;"
    )

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        # only the few lowest-`available_at` groups due now — O(active groups), not a scan
        candidates = await self._db.query(
            f"SELECT group_id, available_at FROM locks WHERE available_at <= $now "
            f"ORDER BY available_at ASC LIMIT {self._CANDIDATES}",
            {"now": now},
        )
        for g in candidates:
            group_id = g["group_id"]
            token = f"{worker_id}:{uuid.uuid4().hex}"
            try:
                await self._db.query(
                    self._ACQUIRE,
                    {"g": group_id, "now": now, "tok": token, "exp": now + visibility},
                )
            except self._SurrealError:
                continue  # another worker leased it first
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
        res = await self._db.query("SELECT token FROM type::thing('locks',$g)", {"g": group_id})
        return bool(res) and res[0].get("token") == token

    async def ack(self, lease: Lease) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        await self._db.query("DELETE messages WHERE seq=$s", {"s": lease.seq})
        remaining = await self._db.query(
            "SELECT seq FROM messages WHERE group_id=$g LIMIT 1", {"g": lease.group_id}
        )
        if remaining:
            await self._db.query(
                "UPDATE type::thing('locks',$g) SET available_at=0, token=NONE", {"g": lease.group_id}
            )
        else:
            await self._db.query("DELETE type::thing('locks',$g)", {"g": lease.group_id})

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            # park: keep the token so the still-present head isn't re-claimed before `delay`
            await self._db.query(
                "UPDATE type::thing('locks',$g) SET available_at=$exp",
                {"g": lease.group_id, "exp": self._clock() + delay},
            )
        else:
            await self._db.query(
                "UPDATE type::thing('locks',$g) SET available_at=0, token=NONE", {"g": lease.group_id}
            )

    async def close(self) -> None:
        await self._db.close()


class AsyncSqsTransport:
    """Native-async `Transport` over AWS SQS **FIFO** via **aioboto3/aiobotocore** — every call
    is awaited on one long-lived aiohttp-backed client, so concurrent workers issue real parallel
    SQS calls. SQS FIFO semantics are unchanged: `MessageGroupId` *is* the per-group exclusivity
    (no other message of a group is delivered while one is in-flight) and the receive visibility
    timeout *is* the lease. publish = send_message(MessageGroupId, MessageDeduplicationId=uuid);
    claim = receive_message(VisibilityTimeout) → the ReceiptHandle is the lease token; ack =
    delete_message; nack(delay) = change_message_visibility(delay).

    Build with `await AsyncSqsTransport.create(...)` (owns its client; `close()` releases it) or
    inject an already-entered aiobotocore client + queue_url via the constructor. The client binds
    to the loop that creates it. Tests mock in-process with `aiomoto`."""

    def __init__(self, client: Any, queue_url: str, wait_seconds: int = 1) -> None:
        self._sqs = client
        self._queue_url = queue_url
        self._wait = wait_seconds
        self._stack: Any = None  # set by create() when this transport owns the client

    @classmethod
    async def create(
        cls,
        endpoint_url: Optional[str] = None,
        queue_name: str = "stm.fifo",
        region: str = "us-east-1",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
        wait_seconds: int = 1,
    ) -> "AsyncSqsTransport":
        """Open an aioboto3 SQS client (LocalStack-friendly: dummy creds + injected
        `endpoint_url`; pass `endpoint_url=None` for real AWS) and ensure the FIFO queue exists,
        retrying until reachable. The client is kept open for the transport's life."""
        import aioboto3
        import anyio
        from botocore.exceptions import BotoCoreError, ClientError

        kwargs: dict[str, Any] = {"region_name": region}
        if endpoint_url is not None:
            kwargs.update(endpoint_url=endpoint_url, aws_access_key_id="test", aws_secret_access_key="test")
        if not queue_name.endswith(".fifo"):
            queue_name += ".fifo"
        stack = AsyncExitStack()
        client = await stack.enter_async_context(aioboto3.Session().client("sqs", **kwargs))
        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                resp = await client.create_queue(QueueName=queue_name, Attributes={"FifoQueue": "true"})
                inst = cls(client, resp["QueueUrl"], wait_seconds)
                inst._stack = stack
                return inst
            except (BotoCoreError, ClientError) as exc:
                last = exc
                await anyio.sleep(retry_delay)
        await stack.aclose()
        raise last if last is not None else RuntimeError("sqs connect failed")

    async def publish(self, group_id: str, event: Event) -> None:
        await self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=event.model_dump_json(),
            MessageGroupId=group_id,
            MessageDeduplicationId=uuid.uuid4().hex,  # unique per send (fan-out reuses event ids)
        )

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        resp = await self._sqs.receive_message(
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

    async def ack(self, lease: Lease) -> None:
        await self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=lease.token)

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        # SQS's native park: hide the message for `delay` seconds (0 = available now)
        await self._sqs.change_message_visibility(
            QueueUrl=self._queue_url, ReceiptHandle=lease.token, VisibilityTimeout=int(delay)
        )

    async def close(self) -> None:
        # release only a client we own (created via create()); an injected client is the caller's
        if self._stack is not None:
            await self._stack.aclose()
            self._stack = None


class AsyncRqliteTransport:
    """Async mirror of `RqliteTransport` over `httpx.AsyncClient`: the same claim
    strategy (one serialized UPDATE leases the oldest deliverable message with a unique
    token, raft ensures sequential consistency) with every HTTP call awaited.
    Build with `await AsyncRqliteTransport.from_url(url)`."""

    def __init__(
        self, client: Any, base_url: str, timeout: float = 10.0, clock: Callable[[], float] = time.time
    ) -> None:
        self._client = client
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._clock = clock

    @classmethod
    async def from_url(
        cls,
        url: str,
        timeout: float = 10.0,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncRqliteTransport":
        import anyio
        import httpx

        last: Exception | None = None
        for _ in range(connect_retries):
            client = httpx.AsyncClient()
            try:
                transport = cls(client, url, timeout)
                await transport._execute(
                    [
                        "CREATE TABLE IF NOT EXISTS messages "
                        "(seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, "
                        "event TEXT NOT NULL, locked_by TEXT, lock_expiry REAL)"
                    ]
                )
                return transport
            except Exception as exc:  # noqa: BLE001
                await client.aclose()
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("rqlite connect failed")

    async def _execute(self, statements: list) -> list:
        resp = await self._client.post(f"{self._base}/db/execute", json=statements, timeout=self._timeout)
        resp.raise_for_status()
        results = resp.json()["results"]
        for res in results:
            if "error" in res:
                raise RuntimeError(f"rqlite execute error: {res['error']}")
        return results

    async def _query(self, sql: str, params: tuple) -> list:
        resp = await self._client.post(
            f"{self._base}/db/query",
            params={"level": "strong"},
            json=[[sql, *params]],
            timeout=self._timeout,
        )
        resp.raise_for_status()
        result = resp.json()["results"][0]
        if "error" in result:
            raise RuntimeError(f"rqlite query error: {result['error']}")
        return result.get("values") or []

    async def publish(self, group_id: str, event: Event) -> None:
        await self._execute(
            [["INSERT INTO messages (group_id, event) VALUES (?, ?)", group_id, event.model_dump_json()]]
        )

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        results = await self._execute(
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
        rows = await self._query("SELECT seq, group_id, event FROM messages WHERE locked_by = ?", (token,))
        seq, group_id, event = rows[0]
        return Lease(seq, group_id, Event.model_validate_json(event), token=token)

    async def ack(self, lease: Lease) -> None:
        await self._execute([["DELETE FROM messages WHERE seq = ?", lease.seq]])

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            await self._execute(
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
            await self._execute(
                [["UPDATE messages SET locked_by = NULL, lock_expiry = 0 WHERE seq = ?", lease.seq]]
            )

    async def close(self) -> None:
        await self._client.aclose()


class AsyncMongoTransport:
    """Async mirror of `MongoTransport` over `motor.motor_asyncio`: per-group exclusivity via
    a per-group `locks` document that is the ready-index + lock in one (`available_at` = next
    claimable epoch, `token` = the lease). `claim` reads only the few lowest `available_at <=
    now` groups (`find(...).sort(available_at).limit(K)`) and atomically leases one — O(log N + K)
    in active groups, not a `$group` over every message. Build with
    `await AsyncMongoTransport.from_url(url)` or inject an `AsyncIOMotorClient`."""

    _CANDIDATES = 8

    def __init__(
        self,
        client: Any,
        db_name: str = "harel",
        prefix: str = "stm",
        clock: Callable[[], float] = time.time,
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
    async def from_url(
        cls,
        url: str,
        db_name: str = "harel",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncMongoTransport":
        import anyio
        import motor.motor_asyncio
        from pymongo.errors import PyMongoError

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                client: Any = motor.motor_asyncio.AsyncIOMotorClient(url)
                await client.admin.command("ping")
                inst = cls(client, db_name)
                await inst._locks.create_index("available_at")  # the claim index
                return inst
            except PyMongoError as exc:
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("mongo connect failed")

    async def _next_seq(self) -> int:
        doc = await self._counters.find_one_and_update(
            {"_id": "seq"}, {"$inc": {"n": 1}}, upsert=True, return_document=self._after
        )
        return int(doc["n"])

    async def publish(self, group_id: str, event: Event) -> None:
        await self._msgs.insert_one(
            {"_id": await self._next_seq(), "group_id": group_id, "event": event.model_dump_json()}
        )
        # ready the group NOW iff it is new ($setOnInsert): don't make an in-flight/parked
        # group claimable before its lease/park elapses
        await self._locks.update_one(
            {"_id": group_id}, {"$setOnInsert": {"available_at": 0.0, "token": None}}, upsert=True
        )

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        # only the few lowest-`available_at` groups due now — O(log N + K), not a scan
        cursor = (
            self._locks.find({"available_at": {"$lte": now}}).sort("available_at", 1).limit(self._CANDIDATES)
        )
        for c in [doc async for doc in cursor]:
            group_id = c["_id"]
            token = f"{worker_id}:{uuid.uuid4().hex}"
            # atomic lease: re-check `available_at <= now` so only one racing worker wins
            leased = await self._locks.find_one_and_update(
                {"_id": group_id, "available_at": {"$lte": now}},
                {"$set": {"token": token, "available_at": now + visibility}},
            )
            if leased is None:
                continue  # another worker leased it first
            head = await self._msgs.find_one({"group_id": group_id}, sort=[("_id", 1)])
            if head is None:
                await self._locks.delete_one({"_id": group_id, "token": token})  # stale group, release
                continue
            return Lease(head["_id"], group_id, Event.model_validate_json(head["event"]), token=token)
        return None

    async def _owns(self, group_id: str, token: str) -> bool:
        doc = await self._locks.find_one({"_id": group_id})
        return doc is not None and doc.get("token") == token

    async def ack(self, lease: Lease) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        await self._msgs.delete_one({"_id": lease.seq})
        if await self._msgs.find_one({"group_id": lease.group_id}) is not None:
            await self._locks.update_one(
                {"_id": lease.group_id, "token": lease.token},
                {"$set": {"available_at": 0.0, "token": None}},
            )
        else:
            await self._locks.delete_one({"_id": lease.group_id, "token": lease.token})

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            # park: keep the token so the still-present head isn't re-claimed before `delay`
            await self._locks.update_one(
                {"_id": lease.group_id, "token": lease.token},
                {"$set": {"available_at": self._clock() + delay}},
            )
        else:
            await self._locks.update_one(
                {"_id": lease.group_id, "token": lease.token},
                {"$set": {"available_at": 0.0, "token": None}},
            )

    async def close(self) -> None:
        self._client.close()
