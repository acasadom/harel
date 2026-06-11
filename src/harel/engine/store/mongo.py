"""MongoStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.store._base import (
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    _decode_offset,
    _encode_offset,
    _matches,
)
from harel.spec.states import Event


class MongoStore:
    """A durable `ExecutionStore` over MongoDB (pymongo) — the document-store
    alternative to the SQL backends, all-network (no shared filesystem). Same
    contract: version/CAS, transactional outbox, dedupe, spawns, timers.

    MongoDB has no multi-document transactions without a replica set, so instead
    of separate collections (as the SQL stores use separate tables) **everything
    for one Execution lives in its single document** — `data` (the serialized
    Execution), plus the `outbox`/`spawns` arrays, the `timers` sub-document and
    the `processed` array. A `commit` is therefore **one `update_one`**, which is
    atomic on a single document with no replica set required — the whole point of
    embedding here.

    Performance note: we never round-trip the whole document. Writes are partial —
    `$set` of `data`/`version`, `$push` to the arrays, `$addToSet`/`$set`/`$unset`
    on the rest — never a full `replace_one`. Reads are projected — `load` pulls
    only `data`; the relay/sweep reads (`pending_outbox`/`pending_spawns`/
    `due_timers`) project only the relevant array/sub-document, never `data`. So a
    growing `data` blob is not dragged through every queue/timer scan.

    The client is injected (duck-typed), so `pymongo` stays an optional dependency
    and tests use mongomock. Collections live under `db_name`: ``executions`` (the
    documents) + ``counters`` (the monotonic outbox/spawn seq allocator)."""

    def __init__(self, client: Any, db_name: str = "harel") -> None:
        from pymongo import ReturnDocument
        from pymongo.errors import DuplicateKeyError

        self._client = client
        self._db = client[db_name]
        self._exes = self._db["executions"]
        self._counters = self._db["counters"]
        self._after = ReturnDocument.AFTER
        self._DuplicateKeyError = DuplicateKeyError

    @classmethod
    def from_url(
        cls, url: str, db_name: str = "harel", connect_retries: int = 30, retry_delay: float = 1.0
    ) -> "MongoStore":
        """Convenience constructor; imports `pymongo` lazily (the optional dep).
        Pings the server, retrying so a worker starting alongside Mongo (compose)
        waits for it to accept connections rather than crashing."""
        import time

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
                time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("mongo connect failed")

    # Node paths use "." as the separator (`Fork.A`); Mongo treats "." in a field
    # name as a path operator, so encode it to a char that cannot appear in a path
    # before using a path as a `timers` sub-document key (reversible).
    @staticmethod
    def _enc(path: str) -> str:
        return path.replace(".", "．")

    @staticmethod
    def _dec(key: str) -> str:
        return key.replace("．", ".")

    def _next_seq(self, name: str, count: int) -> int:
        """Reserve `count` monotonic ids from the `name` counter; return the first.
        A block wasted by a later-aborted commit is harmless (gaps are fine)."""
        doc = self._counters.find_one_and_update(
            {"_id": name}, {"$inc": {"n": count}}, upsert=True, return_document=self._after
        )
        return int(doc["n"]) - count + 1

    def load(self, execution_id: str) -> Optional[Execution]:
        doc = self._exes.find_one({"_id": execution_id}, {"data": 1})
        return Execution.model_validate_json(doc["data"]) if doc is not None else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # definition_id is a top-level field (filtered server-side); status/parent_id
        # live inside the `data` JSON string (filtered client-side). Project only
        # {definition_id, version, data} — never the embedded outbox/spawns/timers arrays.
        status = set(status) if status is not None else None
        query: dict = {} if definition_id is None else {"definition_id": definition_id}
        off = _decode_offset(cursor)
        items: list[ExecutionSummary] = []
        # over-fetch so client-side status/roots filtering still fills the page; if we
        # exhaust the cursor short of `limit`, there simply is no next page.
        cur = self._exes.find(query, {"version": 1, "data": 1}).sort("_id", 1).skip(off)
        scanned = 0
        for doc in cur:
            scanned += 1
            summary = ExecutionSummary.from_data(json.loads(doc["data"]), doc.get("version", 0))
            if _matches(summary, status, definition_id, roots_only):
                items.append(summary)
            if len(items) >= limit:
                break
        nxt = _encode_offset(off + scanned) if len(items) >= limit else None
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
    ) -> None:
        # allocate monotonic seqs up front (one find_one_and_update each; a seq
        # wasted by a lost CAS is harmless), then build the embedded entries
        outbox_entries: list[dict] = []
        if emits:
            base = self._next_seq("outbox", len(emits))
            outbox_entries = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn_entries: list[dict] = []
        if spawns:
            base = self._next_seq("spawn", len(spawns))
            spawn_entries = [
                {"seq": base + i, "parent_id": exe.id, "child_id": cid, "root_path": rp, "context": dict(ctx)}
                for i, (cid, rp, ctx) in enumerate(spawns)
            ]

        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()

        # partial write: $set the core, $push the queues, $addToSet dedupe, and
        # $set/$unset the timer keys — NEVER replace_one the whole document
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
        if push:
            update["$push"] = push
        if processed_event_id is not None:
            update["$addToSet"] = {"processed": processed_event_id}
        if unset_ops:
            update["$unset"] = unset_ops

        res = self._exes.update_one({"_id": exe.id, "version": old}, update)
        if res.matched_count == 1:
            return  # CAS won

        # no document at version=old: either a brand-new Execution or a stale write
        existing = self._exes.find_one({"_id": exe.id}, {"version": 1})
        if existing is None and old == 0:
            doc: dict[str, Any] = {
                "_id": exe.id,
                "definition_id": exe.definition_id,
                "version": exe.version,
                "data": data,
                "outbox": outbox_entries,
                "spawns": spawn_entries,
                "processed": [processed_event_id] if processed_event_id is not None else [],
                "timers": {self._enc(op.path): op.fire_at for op in timers if op.action == "schedule"},
            }
            try:
                self._exes.insert_one(doc)
                return
            except self._DuplicateKeyError:
                existing = self._exes.find_one({"_id": exe.id}, {"version": 1})
        exe.version = old  # undo the in-memory bump; the commit did not happen
        raise StoreConflict(exe.id, expected=old, found=existing["version"] if existing else None)

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        return self._exes.find_one({"_id": execution_id, "processed": event_id}, {"_id": 1}) is not None

    def pending_outbox(self) -> list[OutboxEntry]:
        entries: list[OutboxEntry] = []
        # project only the outbox array (never `data`), oldest first by seq
        for doc in self._exes.find({"outbox": {"$exists": True, "$ne": []}}, {"outbox": 1}):
            for e in doc.get("outbox", []):
                entries.append(OutboxEntry(e["seq"], e["target_id"], Event.model_validate_json(e["event"])))
        return sorted(entries, key=lambda e: e.seq)

    def ack_outbox(self, seq: int) -> None:
        self._exes.update_one({"outbox.seq": seq}, {"$pull": {"outbox": {"seq": seq}}})

    def pending_spawns(self) -> list[SpawnEntry]:
        entries: list[SpawnEntry] = []
        for doc in self._exes.find({"spawns": {"$exists": True, "$ne": []}}, {"spawns": 1}):
            for s in doc.get("spawns", []):
                entries.append(
                    SpawnEntry(s["seq"], s["parent_id"], s["child_id"], s["root_path"], dict(s["context"]))
                )
        return sorted(entries, key=lambda s: s.seq)

    def ack_spawn(self, seq: int) -> None:
        self._exes.update_one({"spawns.seq": seq}, {"$pull": {"spawns": {"seq": seq}}})

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        out: list[tuple[str, str, float]] = []
        # project only the timers sub-document (never `data`)
        for doc in self._exes.find({"timers": {"$exists": True, "$ne": {}}}, {"timers": 1}):
            for enc, fire_at in (doc.get("timers") or {}).items():
                if fire_at <= now:
                    out.append((doc["_id"], self._dec(enc), float(fire_at)))
        return sorted(out, key=lambda t: t[2])

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        # guarded on the stored value: a concurrent re-schedule to a new time wins
        key = f"timers.{self._enc(path)}"
        self._exes.update_one({"_id": execution_id, key: fire_at}, {"$unset": {key: ""}})

    def close(self) -> None:
        self._client.close()
