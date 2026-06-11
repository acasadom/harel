"""AsyncMongoStore — an async ExecutionStore backend."""

from __future__ import annotations

from typing import Any, Optional

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.engine.store._base import DEFAULT_TRACE_MAX
from harel.spec.states import Event


class AsyncMongoStore:
    """Async mirror of `MongoStore` over `motor.motor_asyncio.AsyncIOMotorClient`:
    every collection method is awaited, cursors iterated with `async for`. Same
    single-document CAS (the whole Execution + its embedded outbox/spawns/timers lives
    in one document, so `update_one` with `version=old` filter is atomic without a
    replica set). Build with `await AsyncMongoStore.from_url(url)` or inject an
    already-connected `AsyncIOMotorClient`."""

    def __init__(self, client: Any, db_name: str = "harel") -> None:
        from pymongo import ReturnDocument
        from pymongo.errors import DuplicateKeyError

        self._client = client
        self._db = client[db_name]
        self._exes = self._db["executions"]
        self._counters = self._db["counters"]
        self._after = ReturnDocument.AFTER
        self._DuplicateKeyError = DuplicateKeyError
        self.trace_max = DEFAULT_TRACE_MAX

    @classmethod
    async def from_url(
        cls,
        url: str,
        db_name: str = "harel",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncMongoStore":
        import anyio
        import motor.motor_asyncio
        from pymongo.errors import PyMongoError

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                client: Any = motor.motor_asyncio.AsyncIOMotorClient(url)
                await client.admin.command("ping")
                return cls(client, db_name)
            except PyMongoError as exc:
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("mongo connect failed")

    @staticmethod
    def _enc(path: str) -> str:
        return path.replace(".", "．")

    @staticmethod
    def _dec(key: str) -> str:
        return key.replace("．", ".")

    async def _next_seq(self, name: str, count: int) -> int:
        doc = await self._counters.find_one_and_update(
            {"_id": name}, {"$inc": {"n": count}}, upsert=True, return_document=self._after
        )
        return int(doc["n"]) - count + 1

    async def load(self, execution_id: str) -> Optional[Execution]:
        doc = await self._exes.find_one({"_id": execution_id}, {"data": 1})
        return Execution.model_validate_json(doc["data"]) if doc is not None else None

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one round-trip: an aggregation projects `data` plus a
        server-side `$in` membership flag, so the (growing) `processed` array is never shipped."""
        cursor = self._exes.aggregate(
            [
                {"$match": {"_id": execution_id}},
                {"$project": {"data": 1, "hit": {"$in": [event_id, {"$ifNull": ["$processed", []]}]}}},
            ]
        )
        docs = [d async for d in cursor]
        if not docs:
            return None, False
        return Execution.model_validate_json(docs[0]["data"]), bool(docs[0].get("hit"))

    async def save(self, exe: Execution) -> None:
        await self.commit(exe, [])

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
    ) -> None:
        outbox_entries: list[dict] = []
        if emits:
            base = await self._next_seq("outbox", len(emits))
            outbox_entries = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn_entries: list[dict] = []
        if spawns:
            base = await self._next_seq("spawn", len(spawns))
            spawn_entries = [
                {"seq": base + i, "parent_id": exe.id, "child_id": cid, "root_path": rp, "context": dict(ctx)}
                for i, (cid, rp, ctx) in enumerate(spawns)
            ]
        trace_step: Optional[dict] = None
        if trace is not None:
            trace_step = {**trace, "index": await self._next_seq("trace:" + exe.id, 1) - 1}

        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()

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
        if trace_step is not None:
            push["trace"] = {"$each": [trace_step], **({"$slice": -self.trace_max} if self.trace_max else {})}
        if push:
            update["$push"] = push
        if processed_event_id is not None:
            update["$addToSet"] = {"processed": processed_event_id}
        if unset_ops:
            update["$unset"] = unset_ops

        res = await self._exes.update_one({"_id": exe.id, "version": old}, update)
        if res.matched_count == 1:
            return  # CAS won

        existing = await self._exes.find_one({"_id": exe.id}, {"version": 1})
        if existing is None and old == 0:
            doc: dict[str, Any] = {
                "_id": exe.id,
                "definition_id": exe.definition_id,
                "version": exe.version,
                "data": data,
                "outbox": outbox_entries,
                "spawns": spawn_entries,
                "trace": [trace_step] if trace_step is not None else [],
                "processed": [processed_event_id] if processed_event_id is not None else [],
                "timers": {self._enc(op.path): op.fire_at for op in timers if op.action == "schedule"},
            }
            try:
                await self._exes.insert_one(doc)
                return
            except self._DuplicateKeyError:
                existing = await self._exes.find_one({"_id": exe.id}, {"version": 1})
        exe.version = old
        raise StoreConflict(exe.id, expected=old, found=existing["version"] if existing else None)

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        return (
            await self._exes.find_one({"_id": execution_id, "processed": event_id}, {"_id": 1})
        ) is not None

    async def append_trace(self, execution_id: str, entry: dict) -> None:
        idx = entry.get("index", await self._next_seq("trace:" + execution_id, 1) - 1)
        step = {**entry, "index": idx}
        push = {"$each": [step], **({"$slice": -self.trace_max} if self.trace_max else {})}
        await self._exes.update_one({"_id": execution_id}, {"$push": {"trace": push}}, upsert=True)

    async def read_trace(self, execution_id: str) -> list[dict]:
        doc = await self._exes.find_one({"_id": execution_id}, {"trace": 1})
        return list(doc.get("trace", [])) if doc is not None else []

    async def pending_outbox(self) -> list[OutboxEntry]:
        entries: list[OutboxEntry] = []
        async for doc in self._exes.find({"outbox": {"$exists": True, "$ne": []}}, {"outbox": 1}):
            for e in doc.get("outbox", []):
                entries.append(OutboxEntry(e["seq"], e["target_id"], Event.model_validate_json(e["event"])))
        return sorted(entries, key=lambda e: e.seq)

    async def ack_outbox(self, seq: int) -> None:
        await self._exes.update_one({"outbox.seq": seq}, {"$pull": {"outbox": {"seq": seq}}})

    async def pending_spawns(self) -> list[SpawnEntry]:
        entries: list[SpawnEntry] = []
        async for doc in self._exes.find({"spawns": {"$exists": True, "$ne": []}}, {"spawns": 1}):
            for s in doc.get("spawns", []):
                entries.append(
                    SpawnEntry(s["seq"], s["parent_id"], s["child_id"], s["root_path"], dict(s["context"]))
                )
        return sorted(entries, key=lambda s: s.seq)

    async def ack_spawn(self, seq: int) -> None:
        await self._exes.update_one({"spawns.seq": seq}, {"$pull": {"spawns": {"seq": seq}}})

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        out: list[tuple[str, str, float]] = []
        async for doc in self._exes.find({"timers": {"$exists": True, "$ne": {}}}, {"timers": 1}):
            for enc, fire_at in (doc.get("timers") or {}).items():
                if fire_at <= now:
                    out.append((doc["_id"], self._dec(enc), float(fire_at)))
        return sorted(out, key=lambda t: t[2])

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        key = f"timers.{self._enc(path)}"
        await self._exes.update_one({"_id": execution_id, key: fire_at}, {"$unset": {key: ""}})

    async def close(self) -> None:
        self._client.close()
