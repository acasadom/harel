"""AsyncSqsTransport — an async Transport backend."""

from __future__ import annotations

import uuid
from contextlib import AsyncExitStack
from typing import Any, Optional

from harel.engine.transport import Lease
from harel.engine.transport.sqs import _NO_MIN_PRIORITY, _NO_PRIORITY  # shared reject messages
from harel.spec.states import Event


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

    async def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        if priority:
            raise ValueError(_NO_PRIORITY)
        await self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=event.model_dump_json(),
            MessageGroupId=group_id,
            MessageDeduplicationId=uuid.uuid4().hex,  # unique per send (fan-out reuses event ids)
        )

    async def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        if min_priority:
            raise ValueError(_NO_MIN_PRIORITY)
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
