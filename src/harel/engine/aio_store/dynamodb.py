"""AsyncDynamoDBStore — an async ExecutionStore backend."""

from __future__ import annotations

import json
from contextlib import AsyncExitStack
from typing import Any, Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.engine.store._base import DEFAULT_TRACE_MAX, _decode_offset, _encode_offset, _matches
from harel.engine.store.dynamodb import _PURGE_PARTITIONS
from harel.spec.states import Event


class AsyncDynamoDBStore:
    """Native-async mirror of `DynamoDBStore` over **aioboto3/aiobotocore** — every call is
    awaited on one long-lived aiohttp-backed client, so concurrent workers (`STM_CONCURRENCY`)
    issue real parallel DynamoDB requests (the aiohttp connection pool), not thread-pool-bounded
    ones. Same semantics as the sync store: conditional writes are the CAS
    (`attribute_not_exists(id)` to insert, `version = :ov` to update) and `TransactWriteItems`
    makes the whole `commit` atomic — a stale write cancels the txn (`TransactionCanceledException`)
    and never leaks its outbox. The `boto3` `TypeSerializer`/`TypeDeserializer` are pure (no IO),
    so they are reused as-is.

    The opt-in ``trace`` table (execution_id, idx) caps in the same atomic txn (Put idx=K +
    Delete idx=K-N), keeping exactly the last N **as long as `trace_max` is fixed from the first
    traced commit** (set once at startup); changing it mid-stream over-retains (harmless).

    Build with `await AsyncDynamoDBStore.create(...)` (owns its client; `close()` releases it) or
    inject an already-entered aiobotocore client via the constructor (the caller then owns its
    lifecycle). The client binds to the loop that creates it — build it on the loop you run on
    (e.g. inside `anyio.run`), never share one client across loops. Tests mock in-process with
    `aiomoto` (plain `moto.mock_aws` cannot intercept aiobotocore's aiohttp transport)."""

    def __init__(self, client: Any, prefix: str = "harel") -> None:
        from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
        from botocore.exceptions import ClientError

        self._db = client
        self._prefix = prefix
        self._ser = TypeSerializer()
        self._deser = TypeDeserializer()
        self._ClientError = ClientError
        self._stack: Any = None  # set by create() when this store owns the client
        self.trace_max = DEFAULT_TRACE_MAX

    @classmethod
    async def create(
        cls,
        endpoint_url: Optional[str] = None,
        region: str = "us-east-1",
        prefix: str = "harel",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncDynamoDBStore":
        """Open an aioboto3 client (LocalStack-friendly: dummy creds + injected `endpoint_url`;
        pass `endpoint_url=None` for real AWS) and ensure the tables exist, retrying until the
        endpoint is reachable. The client is kept open for the store's life and released by
        `close()`."""
        import aioboto3
        import anyio
        from botocore.exceptions import BotoCoreError, ClientError

        kwargs: dict[str, Any] = {"region_name": region}
        if endpoint_url is not None:
            kwargs.update(endpoint_url=endpoint_url, aws_access_key_id="test", aws_secret_access_key="test")
        stack = AsyncExitStack()
        client = await stack.enter_async_context(aioboto3.Session().client("dynamodb", **kwargs))
        inst = cls(client, prefix)
        inst._stack = stack
        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                await inst._ensure_tables()
                return inst
            except (BotoCoreError, ClientError) as exc:
                last = exc
                await anyio.sleep(retry_delay)
        await stack.aclose()
        raise last if last is not None else RuntimeError("dynamodb connect failed")

    def _t(self, name: str) -> str:
        return f"{self._prefix}_{name}"

    async def _ensure_tables(self) -> None:
        """Create the tables if absent (idempotent — a pre-existing table is fine)."""
        specs = [
            ("executions", [("id", "S")]),
            ("outbox", [("seq", "N")]),
            ("spawns", [("seq", "N")]),
            ("timers", [("execution_id", "S"), ("path", "S")]),
            ("processed", [("execution_id", "S"), ("event_id", "S")]),
            ("counters", [("id", "S")]),
            ("trace", [("execution_id", "S"), ("idx", "N")]),
        ]
        roles = ["HASH", "RANGE"]
        for name, keys in specs:
            try:
                await self._db.create_table(
                    TableName=self._t(name),
                    KeySchema=[{"AttributeName": k, "KeyType": roles[i]} for i, (k, _) in enumerate(keys)],
                    AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t in keys],
                    BillingMode="PAY_PER_REQUEST",
                )
            except self._ClientError as exc:
                if exc.response["Error"]["Code"] != "ResourceInUseException":
                    raise  # already exists is fine; anything else is real

    def _raw(self, item: dict) -> dict:
        return {k: self._ser.serialize(v) for k, v in item.items()}

    def _item(self, raw: dict) -> dict:
        return {k: self._deser.deserialize(v) for k, v in raw.items()}

    async def _scan(self, table: str, **params: Any) -> list[dict]:
        """Scan a table, following `LastEvaluatedKey` to drain every page (a single Scan
        returns at most 1MB). `params` adds scan options such as a `FilterExpression`."""
        items: list[dict] = []
        kwargs: dict[str, Any] = {"TableName": self._t(table), **params}
        while True:
            resp = await self._db.scan(**kwargs)
            items.extend(self._item(it) for it in resp.get("Items", []))
            start = resp.get("LastEvaluatedKey")
            if not start:
                return items
            kwargs["ExclusiveStartKey"] = start

    async def _next_seq(self, name: str, count: int) -> int:
        """Reserve `count` monotonic ids from the `name` counter (an atomic ADD); return the
        first. A block wasted by a later-cancelled transaction is harmless."""
        resp = await self._db.update_item(
            TableName=self._t("counters"),
            Key=self._raw({"id": name}),
            UpdateExpression="ADD n :k",
            ExpressionAttributeValues={":k": {"N": str(count)}},
            ReturnValues="UPDATED_NEW",
        )
        return int(resp["Attributes"]["n"]["N"]) - count + 1

    async def _max_trace_idx(self, execution_id: str) -> int:
        """The highest trace `idx` for an execution, or -1 if none (a Query, newest first).
        Single-writer-per-execution → the read→write is race-free and idx stays contiguous."""
        resp = await self._db.query(
            TableName=self._t("trace"),
            KeyConditionExpression="execution_id = :e",
            ExpressionAttributeValues={":e": {"S": execution_id}},
            ProjectionExpression="idx",
            ScanIndexForward=False,
            Limit=1,
        )
        items = resp.get("Items")
        return int(self._item(items[0])["idx"]) if items else -1

    async def load(self, execution_id: str) -> Optional[Execution]:
        resp = await self._db.get_item(
            TableName=self._t("executions"),
            Key=self._raw({"id": execution_id}),
            ProjectionExpression="#d",
            ExpressionAttributeNames={"#d": "data"},
        )
        item = resp.get("Item")
        return Execution.model_validate_json(self._item(item)["data"]) if item else None

    async def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        """See `DynamoDBStore.list_executions`: drains the Scan pages (its `Limit` bounds
        items examined, not matched), then pages the matches by offset."""
        status = set(status) if status is not None else None
        kwargs: dict[str, Any] = {
            "TableName": self._t("executions"),
            "ProjectionExpression": "#dat,#v",
            "ExpressionAttributeNames": {"#dat": "data", "#v": "version"},
        }
        if definition_id is not None:
            kwargs["ExpressionAttributeNames"]["#def"] = "definition_id"
            kwargs["FilterExpression"] = "#def = :def"
            kwargs["ExpressionAttributeValues"] = {":def": {"S": definition_id}}
        off = _decode_offset(cursor)
        matched: list[ExecutionSummary] = []
        while True:
            resp = await self._db.scan(**kwargs)
            for raw in resp.get("Items", []):
                item = self._item(raw)
                summary = ExecutionSummary.from_data(json.loads(item["data"]), int(item.get("version", 0)))
                if _matches(summary, status, definition_id, roots_only):
                    matched.append(summary)
            lek = resp.get("LastEvaluatedKey")
            if not lek:
                break
            kwargs["ExclusiveStartKey"] = lek
        nxt = _encode_offset(off + limit) if off + limit < len(matched) else None
        return ExecutionPage(items=matched[off : off + limit], next_cursor=nxt)

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one round-trip: BatchGetItem across the executions and
        processed tables."""
        resp = await self._db.batch_get_item(
            RequestItems={
                self._t("executions"): {
                    "Keys": [self._raw({"id": execution_id})],
                    "ProjectionExpression": "#d",
                    "ExpressionAttributeNames": {"#d": "data"},
                },
                self._t("processed"): {
                    "Keys": [self._raw({"execution_id": execution_id, "event_id": event_id})],
                },
            }
        )
        responses = resp.get("Responses", {})
        exe_items = responses.get(self._t("executions"), [])
        proc_items = responses.get(self._t("processed"), [])
        if not exe_items:
            return None, False
        return Execution.model_validate_json(self._item(exe_items[0])["data"]), bool(proc_items)

    async def save(self, exe: Execution) -> None:
        await self.commit(exe, [])

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,  # execution-trace deferred for this backend (accepted, ignored)
    ) -> list[int]:
        from decimal import Decimal

        # allocate monotonic seqs up front (a seq wasted by a cancelled txn is harmless)
        outbox: list[dict] = []
        if emits:
            base = await self._next_seq("outbox", len(emits))
            outbox = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn: list[dict] = []
        if spawns:
            base = await self._next_seq("spawn", len(spawns))
            spawn = [
                {
                    "seq": base + i,
                    "parent_id": exe.id,
                    "child_id": cid,
                    "root_path": rp,
                    "context": json.dumps(ctx),
                }
                for i, (cid, rp, ctx) in enumerate(spawns)
            ]

        old = exe.version
        exe.version = old + 1
        exe_item = {
            "id": exe.id,
            "data": exe.model_dump_json(),
            "version": exe.version,
            "definition_id": exe.definition_id,
        }
        # the Execution Put carries the CAS: insert iff absent (old==0), else update iff the
        # stored version still matches — a failed condition cancels the whole transaction
        if old == 0:
            cas: dict[str, Any] = {"ConditionExpression": "attribute_not_exists(id)"}
        else:
            cas = {
                "ConditionExpression": "version = :ov",
                "ExpressionAttributeValues": {":ov": {"N": str(old)}},
            }
        txn: list[dict] = [{"Put": {"TableName": self._t("executions"), "Item": self._raw(exe_item), **cas}}]
        for o in outbox:
            txn.append({"Put": {"TableName": self._t("outbox"), "Item": self._raw(o)}})
        for s in spawn:
            txn.append({"Put": {"TableName": self._t("spawns"), "Item": self._raw(s)}})
        if processed_event_id is not None:
            txn.append(
                {
                    "Put": {
                        "TableName": self._t("processed"),
                        "Item": self._raw({"execution_id": exe.id, "event_id": processed_event_id}),
                    }
                }
            )
        for op in timers:
            if op.action == "schedule":
                txn.append(
                    {
                        "Put": {
                            "TableName": self._t("timers"),
                            "Item": self._raw(
                                {"execution_id": exe.id, "path": op.path, "fire_at": Decimal(str(op.fire_at))}
                            ),
                        }
                    }
                )
            else:
                txn.append(
                    {
                        "Delete": {
                            "TableName": self._t("timers"),
                            "Key": self._raw({"execution_id": exe.id, "path": op.path}),
                        }
                    }
                )

        if trace is not None:
            idx = await self._max_trace_idx(exe.id) + 1  # contiguous per execution (read max, +1)
            txn.append(
                {
                    "Put": {
                        "TableName": self._t("trace"),
                        "Item": self._raw({"execution_id": exe.id, "idx": idx, "entry": json.dumps(trace)}),
                    }
                }
            )
            if self.trace_max and idx - self.trace_max >= 0:  # ring: drop the item leaving the window
                txn.append(
                    {
                        "Delete": {
                            "TableName": self._t("trace"),
                            "Key": self._raw({"execution_id": exe.id, "idx": idx - self.trace_max}),
                        }
                    }
                )

        try:
            await self._db.transact_write_items(TransactItems=txn)
        except self._ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in ("TransactionCanceledException", "ConditionalCheckFailedException"):
                raise  # a real error, not a CAS miss
            exe.version = old  # undo the in-memory bump; the txn was cancelled
            resp = await self._db.get_item(
                TableName=self._t("executions"),
                Key=self._raw({"id": exe.id}),
                ProjectionExpression="version",
            )
            found = int(self._item(resp["Item"])["version"]) if "Item" in resp else None
            raise StoreConflict(exe.id, expected=old, found=found)
        return [o["seq"] for o in outbox]

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        resp = await self._db.get_item(
            TableName=self._t("processed"),
            Key=self._raw({"execution_id": execution_id, "event_id": event_id}),
        )
        return "Item" in resp

    async def append_trace(self, execution_id: str, entry: dict) -> None:
        idx = entry.get("index", await self._max_trace_idx(execution_id) + 1)
        await self._db.put_item(
            TableName=self._t("trace"),
            Item=self._raw({"execution_id": execution_id, "idx": idx, "entry": json.dumps(entry)}),
        )
        if self.trace_max and idx - self.trace_max >= 0:
            await self._db.delete_item(
                TableName=self._t("trace"),
                Key=self._raw({"execution_id": execution_id, "idx": idx - self.trace_max}),
            )

    async def read_trace(self, execution_id: str) -> list[dict]:
        resp = await self._db.query(
            TableName=self._t("trace"),
            KeyConditionExpression="execution_id = :e",
            ExpressionAttributeValues={":e": {"S": execution_id}},
            ScanIndexForward=True,  # oldest → newest
        )
        out = []
        for raw in resp.get("Items", []):
            it = self._item(raw)
            out.append({**json.loads(it["entry"]), "index": int(it["idx"])})
        return out

    async def pending_outbox(self) -> list[OutboxEntry]:
        rows = await self._scan("outbox")
        rows.sort(key=lambda r: int(r["seq"]))  # Scan is unordered; sort by seq
        return [
            OutboxEntry(int(r["seq"]), r.get("target_id"), Event.model_validate_json(r["event"]))
            for r in rows
        ]

    async def ack_outbox(self, seq: int) -> None:
        await self._db.delete_item(TableName=self._t("outbox"), Key=self._raw({"seq": seq}))

    async def pending_spawns(self) -> list[SpawnEntry]:
        rows = await self._scan("spawns")
        rows.sort(key=lambda r: int(r["seq"]))
        return [
            SpawnEntry(int(r["seq"]), r["parent_id"], r["child_id"], r["root_path"], json.loads(r["context"]))
            for r in rows
        ]

    async def ack_spawn(self, seq: int) -> None:
        await self._db.delete_item(TableName=self._t("spawns"), Key=self._raw({"seq": seq}))

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = await self._scan(
            "timers",
            FilterExpression="fire_at <= :now",
            ExpressionAttributeValues={":now": {"N": str(now)}},
        )
        out = [(r["execution_id"], r["path"], float(r["fire_at"])) for r in rows]
        return sorted(out, key=lambda t: t[2])

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        from decimal import Decimal

        # guarded on the stored value: a concurrent re-schedule to a new time wins
        try:
            await self._db.delete_item(
                TableName=self._t("timers"),
                Key=self._raw({"execution_id": execution_id, "path": path}),
                ConditionExpression="fire_at = :f",
                ExpressionAttributeValues={":f": {"N": str(Decimal(str(fire_at)))}},
            )
        except self._ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise  # the guard didn't match (stale sweep) — a no-op, as intended

    async def ids_with_prefix(self, prefix: str) -> list[str]:
        rows = await self._scan(
            "executions",
            FilterExpression="begins_with(#i, :p)",
            ExpressionAttributeNames={"#i": "id"},
            ExpressionAttributeValues={":p": {"S": prefix}},
            ProjectionExpression="#i",
        )
        return [r["id"] for r in rows]

    async def purge(self, execution_id: str, expected_version: int) -> bool:
        try:
            await self._db.delete_item(
                TableName=self._t("executions"),
                Key=self._raw({"id": execution_id}),
                ConditionExpression="version = :v",
                ExpressionAttributeValues={":v": {"N": str(expected_version)}},
            )
            deleted = True
        except self._ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            resp = await self._db.get_item(
                TableName=self._t("executions"),
                Key=self._raw({"id": execution_id}),
                ProjectionExpression="id",
            )
            if "Item" in resp:
                return False  # moved on: touch nothing
            deleted = False
        # not atomic with the Execution delete — see DynamoDBStore.purge
        for table, sort_key in _PURGE_PARTITIONS:
            await self._delete_keys(table, await self._partition_keys(table, sort_key, execution_id))
        for table, attr in (("outbox", "target_id"), ("spawns", "parent_id")):
            rows = await self._scan(
                table,
                FilterExpression="#a = :v",
                ExpressionAttributeNames={"#a": attr},
                ExpressionAttributeValues={":v": {"S": execution_id}},
                ProjectionExpression="seq",
            )
            await self._delete_keys(table, [self._raw({"seq": r["seq"]}) for r in rows])
        return deleted

    async def _partition_keys(self, table: str, sort_key: str, execution_id: str) -> list[dict]:
        kwargs: dict[str, Any] = {
            "TableName": self._t(table),
            "KeyConditionExpression": "execution_id = :e",
            "ExpressionAttributeValues": {":e": {"S": execution_id}},
            "ProjectionExpression": "execution_id, #sk",
            "ExpressionAttributeNames": {"#sk": sort_key},
        }
        keys: list[dict] = []
        while True:
            resp = await self._db.query(**kwargs)
            keys.extend(resp.get("Items", []))
            start = resp.get("LastEvaluatedKey")
            if not start:
                return keys
            kwargs["ExclusiveStartKey"] = start

    async def _delete_keys(self, table: str, keys: list[dict]) -> None:
        for i in range(0, len(keys), 25):
            pending = {self._t(table): [{"DeleteRequest": {"Key": k}} for k in keys[i : i + 25]]}
            while pending:
                pending = (await self._db.batch_write_item(RequestItems=pending)).get(
                    "UnprocessedItems"
                ) or {}

    async def close(self) -> None:
        # release only a client we own (created via create()); an injected client is the
        # caller's to close
        if self._stack is not None:
            await self._stack.aclose()
            self._stack = None
