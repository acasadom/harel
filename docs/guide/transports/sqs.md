# SqsTransport — AWS SQS FIFO (native fit)

AWS SQS **FIFO** gives the transport's whole invariant for free, so this is the thinnest backend.
A FIFO queue's `MessageGroupId` **is** the per-group exclusivity (SQS will not deliver another
message of a group while one is in flight), and the receive **visibility timeout is the lease**.
There is no lock table and no ready-index — AWS runs the exclusivity. Works against real SQS or
**LocalStack** (no AWS account) by pointing `endpoint_url` at it; `boto3` is an optional extra.
It's the all-AWS partner of [`DynamoDBStore`](../stores/dynamodb).

## No data model of our own

There is no schema — the queue is the SQS FIFO queue itself. `create(...)` ensures the queue
exists (`create_queue(QueueName, Attributes={"FifoQueue": "true"})`, appending `.fifo` to the name
if needed). The `Lease`'s handle is the SQS **`ReceiptHandle`** returned by `receive_message` (its
`token`).

## The mapping

```text
publish(group_id, event):
    send_message(QueueUrl, MessageBody=event_json,
                 MessageGroupId=group_id,                 # the per-group exclusivity key
                 MessageDeduplicationId=uuid())           # unique per send (fan-out reuses event ids)

claim(worker_id, visibility):
    receive_message(MaxNumberOfMessages=1,
                    VisibilityTimeout=int(visibility),     # the lease: hidden from others until it elapses
                    WaitTimeSeconds=wait,                  # long-poll
                    AttributeNames=["MessageGroupId"])
    -> Lease(group_id=msg.Attributes.MessageGroupId, event=msg.Body, token=msg.ReceiptHandle)

ack(lease):           delete_message(QueueUrl, ReceiptHandle=lease.token)             # remove it
nack(lease, delay=0): change_message_visibility(ReceiptHandle=lease.token,
                                                VisibilityTimeout=int(delay))         # 0 = available now
close():              the boto3 client
```

- **Exclusivity & FIFO** are AWS's: within a `MessageGroupId`, messages are delivered in order and
  only one is in flight at a time — exactly harel's single-active-consumer-per-group invariant,
  natively.
- **The lease** is the visibility timeout: a received message is hidden for `VisibilityTimeout`
  seconds; if the worker dies (no `delete`), it reappears — crash recovery, AWS-managed.
- **`MessageDeduplicationId`** is a fresh uuid per send (NOT the event id): a fan-out re-uses event
  ids across its children, and SQS FIFO would dedupe identical dedup-ids within the 5-minute
  window, so a per-send uuid keeps every publish distinct.
- **nack / park** maps to `change_message_visibility`: `delay>0` re-hides the message for `delay`
  seconds (the park the [control plane](../control-plane) uses for a suspended group); `delay=0`
  makes it available immediately (retry).
- **No priority.** SQS FIFO has no per-group priority and `receive_message` can't filter by one, so
  `SqsTransport` **rejects** `publish(..., priority>0)` and `claim(..., min_priority>0)` with a
  clear error (fail-fast) rather than silently dropping them — a priority-routed worker
  (`high_ratio>0`) would otherwise be a no-op on SQS. Round-robin fairness across groups is still
  provided natively by `MessageGroupId` delivery. Use another transport for priority routing.

## Async twin

`AsyncSqsTransport` mirrors this over native-async aioboto3/aiobotocore — the same
`send`/`receive`/`delete`/`change_visibility`, every call awaited.

## When to pick it

A fully serverless, no-server AWS stack — pair it with [DynamoDBStore](../stores/dynamodb). One
caveat worth knowing: SQS can't purge or prioritise a queue, which is why harel's cooperative
`cancel` drains the backlog as no-ops rather than relying on a queue jump (see the
[control plane](../control-plane)). See the [transports hub](../transports) and
[distribution](../distribution).
