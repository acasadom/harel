"""SqsTransport — a Transport backend."""

from __future__ import annotations

import uuid
from typing import Any, Optional

from harel.engine.transport._base import Lease
from harel.spec.states import Event


class SqsTransport:
    """`Transport` over AWS SQS **FIFO** — the native fit: SQS's `MessageGroupId`
    *is* the per-group exclusivity (no other message of a group is delivered while
    one is in-flight) and the receive **visibility timeout** *is* the lease. Works
    against real SQS or **LocalStack** (no AWS account) — just point `endpoint_url`
    at it. `boto3` is an optional extra; the client is injected.

    publish = send_message(MessageGroupId, MessageDeduplicationId=uuid); claim =
    receive_message(VisibilityTimeout) → the ReceiptHandle is the lease (`token`);
    ack = delete_message; nack = change_message_visibility(0)."""

    def __init__(self, client: Any, queue_url: str, wait_seconds: int = 1) -> None:
        self._sqs = client
        self._queue_url = queue_url
        self._wait = wait_seconds

    @classmethod
    def create(
        cls,
        endpoint_url: str,
        queue_name: str = "stm.fifo",
        region: str = "us-east-1",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "SqsTransport":
        """Build a client (LocalStack-friendly: dummy creds, injected endpoint) and
        ensure the FIFO queue exists, retrying until the endpoint is reachable."""
        import time as _time

        import boto3
        from botocore.exceptions import BotoCoreError, ClientError

        client = boto3.client(
            "sqs",
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        if not queue_name.endswith(".fifo"):
            queue_name += ".fifo"
        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                resp = client.create_queue(QueueName=queue_name, Attributes={"FifoQueue": "true"})
                return cls(client, resp["QueueUrl"])
            except (BotoCoreError, ClientError) as exc:
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("sqs connect failed")

    def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=event.model_dump_json(),
            MessageGroupId=group_id,
            MessageDeduplicationId=uuid.uuid4().hex,  # unique per send (fan-out reuses event ids)
        )

    def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        resp = self._sqs.receive_message(
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

    def ack(self, lease: Lease) -> None:
        self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=lease.token)

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        # SQS's native park: hide the message for `delay` seconds (0 = available now)
        self._sqs.change_message_visibility(
            QueueUrl=self._queue_url, ReceiptHandle=lease.token, VisibilityTimeout=int(delay)
        )

    def close(self) -> None:
        self._sqs.close()
