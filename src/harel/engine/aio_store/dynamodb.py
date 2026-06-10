"""AsyncDynamoDBStore — an async ExecutionStore backend."""

from __future__ import annotations

import json
from contextlib import AsyncExitStack
from typing import Any, Optional

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
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

    async def load(self, execution_id: str) -> Optional[Execution]:
        resp = await self._db.get_item(
            TableName=self._t("executions"),
            Key=self._raw({"id": execution_id}),
            ProjectionExpression="#d",
            ExpressionAttributeNames={"#d": "data"},
        )
        item = resp.get("Item")
        return Execution.model_validate_json(self._item(item)["data"]) if item else None

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
    ) -> None:
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

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        resp = await self._db.get_item(
            TableName=self._t("processed"),
            Key=self._raw({"execution_id": execution_id, "event_id": event_id}),
        )
        return "Item" in resp

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

    async def close(self) -> None:
        # release only a client we own (created via create()); an injected client is the
        # caller's to close
        if self._stack is not None:
            await self._stack.aclose()
            self._stack = None
