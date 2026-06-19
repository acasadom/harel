"""AsyncMongoTransport — an async Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport import Lease
from harel.spec.states import Event


class AsyncMongoTransport:
    """Async mirror of `MongoTransport` over `motor.motor_asyncio`: per-group exclusivity via
    a per-group `locks` document that is the ready-index + lock in one (`available_at` = next
    claimable epoch, `token` = the lease). `claim` leases the lowest-`available_at <= now` group
    in ONE atomic `find_one_and_update` (`sort=available_at`) — O(log N) in active groups, not a
    `$group` over every message, and concurrent claimers each get a DISTINCT group (no lost-lease
    races). Build with `await AsyncMongoTransport.from_url(url)` or inject an `AsyncIOMotorClient`."""

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
        while True:
            token = f"{worker_id}:{uuid.uuid4().hex}"
            # ONE atomic op: find the lowest-`available_at` due group AND lease it (sort +
            # find_one_and_update). Concurrent claimers each get a DISTINCT group — the update
            # bumps `available_at` out of range, so no two race for the same head (no lost
            # leases). Replaces a find()-then-loop-of-find_one_and_update where workers fished
            # the same candidate window and burned round-trips on lost leases.
            leased = await self._locks.find_one_and_update(
                {"available_at": {"$lte": now}},
                {"$set": {"token": token, "available_at": now + visibility}},
                sort=[("available_at", 1)],
            )
            if leased is None:
                return None  # nothing due
            group_id = leased["_id"]
            head = await self._msgs.find_one({"group_id": group_id}, sort=[("_id", 1)])
            if head is None:
                await self._locks.delete_one({"_id": group_id, "token": token})  # stale empty group
                continue
            return Lease(head["_id"], group_id, Event.model_validate_json(head["event"]), token=token)

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
