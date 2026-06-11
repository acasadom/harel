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
