"""MongoTransport — a Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport._base import Lease
from harel.spec.states import Event


class MongoTransport:
    """`Transport` over MongoDB — a multi-machine queue (the document-store
    sibling of the SQL queues), no Redis. MongoDB has no native message groups,
    so — like `RedisTransport` — the per-group exclusivity is built by hand:

    - ``{prefix}_messages`` — the FIFO, one document per message keyed by a
      monotonic `_id` seq (oldest = smallest seq).
    - ``{prefix}_locks`` — one document per group that has messages, the
      **ready-index + lock in one**: `available_at` is the epoch at which the
      group is next claimable (0 = now), and `token` is the current lease (for
      fencing). `claim` leases the lowest-`available_at <= now` group in ONE atomic
      `find_one_and_update` (`sort=available_at`), so its cost is O(log N) in the number
      of *active groups* — NOT a `$group` aggregation over every message (the old design,
      which scanned the whole `messages` collection on each claim and collapsed under a
      backlog). Leasing bumps `available_at` to `now + visibility`, so concurrent claimers
      each get a DISTINCT group (no lost-lease races) AND a group reappears on its own
      once the lease expires (crash recovery, no separate sweep).

    `claim`'s single sorted `find_one_and_update` picks-and-leases the head group
    atomically (server-side; replaced a `find().sort().limit(K)`-then-loop where workers
    raced for the same candidate window) and returns its head without removing it; `ack`
    (lock still owned) deletes the head and re-readies the group (`available_at=0`) or
    drops it if empty; `nack` re-readies now, or parks it for `delay`. The client is
    injected (duck-typed), so `pymongo` is an optional extra and tests use mongomock.
    NOTE: like the other lease backends, a lock that expires mid-ack can let two workers
    touch one group; the store's version/CAS is the backstop."""

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
        while True:
            token = f"{worker_id}:{uuid.uuid4().hex}"
            # ONE atomic op: find the lowest-`available_at` due group AND lease it (sort +
            # find_one_and_update). Concurrent claimers each get a DISTINCT group — the update
            # bumps `available_at` out of range, so no two race for the same head (no lost
            # leases). Replaces a find()-then-loop-of-find_one_and_update where workers fished
            # the same candidate window and burned round-trips on lost leases.
            leased = self._locks.find_one_and_update(
                {"available_at": {"$lte": now}},
                {"$set": {"token": token, "available_at": now + visibility}},
                sort=[("available_at", 1)],
            )
            if leased is None:
                return None  # nothing due
            group_id = leased["_id"]
            head = self._msgs.find_one({"group_id": group_id}, sort=[("_id", 1)])
            if head is None:
                self._locks.delete_one({"_id": group_id, "token": token})  # stale empty group
                continue
            return Lease(head["_id"], group_id, Event.model_validate_json(head["event"]), token=token)

    def _owns(self, group_id: str, token: str) -> bool:
        doc = self._locks.find_one({"_id": group_id})
        return doc is not None and doc.get("token") == token

    def ack(self, lease: Lease) -> None:
        if not self._owns(lease.group_id, lease.token):
            return  # fencing: only the current lock holder removes + re-readies
        self._msgs.delete_one({"_id": lease.seq})
        if self._msgs.find_one({"group_id": lease.group_id}) is not None:
            # score = now so this group goes to the back of the ready queue (round-robin)
            self._locks.update_one(
                {"_id": lease.group_id, "token": lease.token},
                {"$set": {"available_at": self._clock(), "token": None}},
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
