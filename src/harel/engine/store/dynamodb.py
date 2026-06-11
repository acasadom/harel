"""DynamoDBStore — a durable ExecutionStore backend."""

from __future__ import annotations

import base64
import json
from decimal import Decimal
from typing import Any, Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.store._base import (
    DEFAULT_TRACE_MAX,
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    _matches,
)
from harel.spec.states import Event


class DynamoDBStore:
    """A durable `ExecutionStore` over AWS DynamoDB (boto3) — the serverless
    sibling of the SQL backends, and the natural store-side partner of
    `SqsTransport` for an **all-AWS, no-server** stack. Runs against real DynamoDB
    or **LocalStack**/**moto** (no AWS account); the client is injected, so `boto3`
    stays an optional extra.

    DynamoDB gives the two primitives directly: conditional writes are the CAS
    (`attribute_not_exists(id)` to insert, `version = :old` to update) and
    `TransactWriteItems` makes the whole `commit` atomic across items — the
    Execution Put (CAS-conditioned) plus the outbox/spawns/processed/timers writes
    either all apply or none, so a stale write never leaks its outbox. A failed CAS
    cancels the transaction (`TransactionCanceledException`; older moto raises
    `ConditionalCheckFailedException`, so we treat both as a conflict).

    Tables (created idempotently, prefixed by `prefix`): ``executions`` (id),
    ``outbox``/``spawns`` (seq), ``timers`` (execution_id, path), ``processed``
    (execution_id, event_id), ``counters`` (the monotonic seq allocator), and
    ``trace`` (execution_id, idx) for the opt-in timeline. The trace ring caps in
    the same atomic txn (Put idx=K + Delete idx=K-N), which keeps exactly the last
    N **as long as `trace_max` is fixed from the first traced commit** (the
    production case — set once at startup); changing it mid-stream over-retains
    (harmless) until old items age out, rather than trimming immediately.

    Trade-offs of the document model, deliberately accepted: the relay/sweep reads
    (`pending_outbox`/`pending_spawns`/`due_timers`) use `Scan` + a client-side
    sort (DynamoDB Scan is unordered) — fine because those tables drain and stay
    near-empty. `TransactWriteItems` caps a single commit at 100 items / 4MB, far
    above a normal commit (1 Execution + a handful of emits/spawns/timers)."""

    def __init__(self, client: Any, prefix: str = "harel") -> None:
        from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
        from botocore.exceptions import ClientError

        self._db = client
        self._prefix = prefix
        self._ser = TypeSerializer()
        self._deser = TypeDeserializer()
        self._ClientError = ClientError
        self.trace_max = DEFAULT_TRACE_MAX
        self._ensure_tables()

    @classmethod
    def create(
        cls,
        endpoint_url: Optional[str] = None,
        region: str = "us-east-1",
        prefix: str = "harel",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "DynamoDBStore":
        """Build a boto3 client (LocalStack-friendly: dummy creds + injected
        `endpoint_url`; pass `endpoint_url=None` for real AWS) and ensure the
        tables exist, retrying until the endpoint is reachable."""
        import time

        import boto3
        from botocore.exceptions import BotoCoreError, ClientError

        kwargs: dict[str, Any] = {"region_name": region}
        if endpoint_url is not None:
            kwargs.update(endpoint_url=endpoint_url, aws_access_key_id="test", aws_secret_access_key="test")
        client = boto3.client("dynamodb", **kwargs)
        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(client, prefix)
            except (BotoCoreError, ClientError) as exc:
                last = exc
                time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("dynamodb connect failed")

    def _t(self, name: str) -> str:
        return f"{self._prefix}_{name}"

    def _ensure_tables(self) -> None:
        """Create the tables if absent (idempotent — a pre-existing table is fine).
        Convenient for LocalStack/moto/dev; on real AWS the tables usually pre-exist."""
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
                self._db.create_table(
                    TableName=self._t(name),
                    KeySchema=[{"AttributeName": k, "KeyType": roles[i]} for i, (k, _) in enumerate(keys)],
                    AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t in keys],
                    BillingMode="PAY_PER_REQUEST",
                )
            except self._ClientError as exc:
                if exc.response["Error"]["Code"] != "ResourceInUseException":
                    raise  # already exists is fine; anything else is real

    def _raw(self, item: dict) -> dict:
        """A native dict → DynamoDB's typed attribute-value form."""
        return {k: self._ser.serialize(v) for k, v in item.items()}

    def _item(self, raw: dict) -> dict:
        """DynamoDB's typed form → a native dict (numbers come back as Decimal)."""
        return {k: self._deser.deserialize(v) for k, v in raw.items()}

    def _scan(self, table: str, **params: Any) -> list[dict]:
        """Scan a table, following `LastEvaluatedKey` to drain every page (a single Scan
        returns at most 1MB). `params` adds scan options such as a `FilterExpression`."""
        items: list[dict] = []
        kwargs: dict[str, Any] = {"TableName": self._t(table), **params}
        while True:
            resp = self._db.scan(**kwargs)
            items.extend(self._item(it) for it in resp.get("Items", []))
            start = resp.get("LastEvaluatedKey")
            if not start:
                return items
            kwargs["ExclusiveStartKey"] = start

    def _next_seq(self, name: str, count: int) -> int:
        """Reserve `count` monotonic ids from the `name` counter (an atomic ADD);
        return the first. A block wasted by a later-cancelled transaction is harmless."""
        resp = self._db.update_item(
            TableName=self._t("counters"),
            Key=self._raw({"id": name}),
            UpdateExpression="ADD n :k",
            ExpressionAttributeValues={":k": {"N": str(count)}},
            ReturnValues="UPDATED_NEW",
        )
        return int(resp["Attributes"]["n"]["N"]) - count + 1

    def _max_trace_idx(self, execution_id: str) -> int:
        """The highest trace `idx` for an execution, or -1 if none. A Query on the partition
        key, newest first — single-writer-per-execution, so the read→write is race-free and the
        index stays contiguous (no counter, so a cancelled commit leaves no gap)."""
        resp = self._db.query(
            TableName=self._t("trace"),
            KeyConditionExpression="execution_id = :e",
            ExpressionAttributeValues={":e": {"S": execution_id}},
            ProjectionExpression="idx",
            ScanIndexForward=False,
            Limit=1,
        )
        items = resp.get("Items")
        return int(self._item(items[0])["idx"]) if items else -1

    def load(self, execution_id: str) -> Optional[Execution]:
        resp = self._db.get_item(
            TableName=self._t("executions"),
            Key=self._raw({"id": execution_id}),
            ProjectionExpression="#d",
            ExpressionAttributeNames={"#d": "data"},
        )
        item = resp.get("Item")
        return Execution.model_validate_json(self._item(item)["data"]) if item else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # Scan (unordered) projecting only data+version; status/parent_id are inside the
        # `data` JSON so they're filtered client-side (definition_id can be a server-side
        # FilterExpression). cursor = base64 of the native LastEvaluatedKey. `Limit` bounds
        # items EXAMINED before the filter, so a page may return fewer than `limit` matches.
        status = set(status) if status is not None else None
        kwargs: dict[str, Any] = {
            "TableName": self._t("executions"),
            "ProjectionExpression": "#dat,#v",
            "ExpressionAttributeNames": {"#dat": "data", "#v": "version"},
            "Limit": limit,
        }
        if definition_id is not None:
            kwargs["ExpressionAttributeNames"]["#def"] = "definition_id"
            kwargs["FilterExpression"] = "#def = :def"
            kwargs["ExpressionAttributeValues"] = {":def": {"S": definition_id}}
        if cursor:
            kwargs["ExclusiveStartKey"] = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        resp = self._db.scan(**kwargs)
        items = []
        for raw in resp.get("Items", []):
            item = self._item(raw)
            summary = ExecutionSummary.from_data(json.loads(item["data"]), int(item.get("version", 0)))
            if _matches(summary, status, definition_id, roots_only):
                items.append(summary)
        lek = resp.get("LastEvaluatedKey")
        nxt = base64.urlsafe_b64encode(json.dumps(lek).encode()).decode() if lek else None
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
        trace: Optional[dict] = None,  # execution-trace deferred for this backend (accepted, ignored)
    ) -> None:
        # allocate monotonic seqs up front (a seq wasted by a cancelled txn is harmless)
        outbox: list[dict] = []
        if emits:
            base = self._next_seq("outbox", len(emits))
            outbox = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn: list[dict] = []
        if spawns:
            base = self._next_seq("spawn", len(spawns))
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

        # the Execution Put carries the CAS: insert iff absent (old==0), else update
        # iff the stored version still matches — a failed condition cancels the txn
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
            idx = self._max_trace_idx(exe.id) + 1  # contiguous per execution (read max, +1)
            txn.append(
                {
                    "Put": {
                        "TableName": self._t("trace"),
                        "Item": self._raw({"execution_id": exe.id, "idx": idx, "entry": json.dumps(trace)}),
                    }
                }
            )
            # ring: drop the item that falls out of the last-N window (Delete of an absent key
            # is a no-op), all in the same atomic TransactWriteItems
            if self.trace_max and idx - self.trace_max >= 0:
                txn.append(
                    {
                        "Delete": {
                            "TableName": self._t("trace"),
                            "Key": self._raw({"execution_id": exe.id, "idx": idx - self.trace_max}),
                        }
                    }
                )

        try:
            self._db.transact_write_items(TransactItems=txn)
        except self._ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in ("TransactionCanceledException", "ConditionalCheckFailedException"):
                raise  # a real error, not a CAS miss
            exe.version = old  # undo the in-memory bump; the txn was cancelled
            resp = self._db.get_item(
                TableName=self._t("executions"),
                Key=self._raw({"id": exe.id}),
                ProjectionExpression="version",
            )
            found = int(self._item(resp["Item"])["version"]) if "Item" in resp else None
            raise StoreConflict(exe.id, expected=old, found=found)

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        resp = self._db.get_item(
            TableName=self._t("processed"),
            Key=self._raw({"execution_id": execution_id, "event_id": event_id}),
        )
        return "Item" in resp

    def append_trace(self, execution_id: str, entry: dict) -> None:
        idx = entry.get("index", self._max_trace_idx(execution_id) + 1)
        self._db.put_item(
            TableName=self._t("trace"),
            Item=self._raw({"execution_id": execution_id, "idx": idx, "entry": json.dumps(entry)}),
        )
        if self.trace_max and idx - self.trace_max >= 0:
            self._db.delete_item(
                TableName=self._t("trace"),
                Key=self._raw({"execution_id": execution_id, "idx": idx - self.trace_max}),
            )

    def read_trace(self, execution_id: str) -> list[dict]:
        resp = self._db.query(
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

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._scan("outbox")
        rows.sort(key=lambda r: int(r["seq"]))  # Scan is unordered; sort by seq
        return [
            OutboxEntry(int(r["seq"]), r.get("target_id"), Event.model_validate_json(r["event"]))
            for r in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._db.delete_item(TableName=self._t("outbox"), Key=self._raw({"seq": seq}))

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._scan("spawns")
        rows.sort(key=lambda r: int(r["seq"]))
        return [
            SpawnEntry(int(r["seq"]), r["parent_id"], r["child_id"], r["root_path"], json.loads(r["context"]))
            for r in rows
        ]

    def ack_spawn(self, seq: int) -> None:
        self._db.delete_item(TableName=self._t("spawns"), Key=self._raw({"seq": seq}))

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._scan(
            "timers",
            FilterExpression="fire_at <= :now",
            ExpressionAttributeValues={":now": {"N": str(now)}},
        )
        out = [(r["execution_id"], r["path"], float(r["fire_at"])) for r in rows]
        return sorted(out, key=lambda t: t[2])

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        # guarded on the stored value: a concurrent re-schedule to a new time wins
        try:
            self._db.delete_item(
                TableName=self._t("timers"),
                Key=self._raw({"execution_id": execution_id, "path": path}),
                ConditionExpression="fire_at = :f",
                ExpressionAttributeValues={":f": {"N": str(Decimal(str(fire_at)))}},
            )
        except self._ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise  # the guard didn't match (stale sweep) — a no-op, as intended

    def close(self) -> None:
        self._db.close()
