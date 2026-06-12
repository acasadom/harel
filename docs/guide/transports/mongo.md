# MongoTransport — per-group ready-index document

A multi-machine queue on MongoDB (the document-store sibling of the SQL queues), no Redis. Like
[`RedisTransport`](redis), MongoDB has no native message groups, so per-group exclusivity is built
by hand with a **per-group lock document** that is the ready-index and the lease in one. `claim`
reads only the few lowest-`available_at` due groups, so its cost is `O(log N + K)` — never a
`$group` aggregation over every message. The client is injected (`pymongo` optional; tests use
mongomock); pairs with [`MongoStore`](../stores/mongo) for an all-Mongo stack.

## Collections (under `db_name`, names prefixed)

```text
{prefix}_messages  one doc per message: { _id: seq, group_id, event }   (seq = monotonic; oldest = smallest)
{prefix}_locks     one doc per active group: { _id: group_id, available_at: float, token: str|None }
{prefix}_counters  the monotonic seq allocator ({_id:"seq"}, $inc n)
```

- **`_messages`** is the FIFO: `_id` is a monotonic seq from `_next_seq` (a `find_one_and_update`
  `$inc`), so the head of a group is the smallest `_id`.
- **`_locks`** is the ready-index **and** the lock in one document per group: `available_at` is
  the epoch at which the group is next claimable (0 = now), `token` is the current lease (a
  `worker_id:uuid` fencing token). An index on `available_at` makes `claim` `O(log N + K)`.

## publish

```text
_messages.insert_one({ _id: next_seq(), group_id, event: event_json })
_locks.update_one({_id: group_id}, {$setOnInsert: {available_at: 0.0, token: None}}, upsert=True)
```

`$setOnInsert` readies the group **only if new** (a brand-new group is claimable now, score 0); a
publish into an in-flight or parked group must not pull its `available_at` back and make it
claimable before its lease/park elapses (the Mongo analogue of Redis `ZADD … NX`).

## claim — atomic lease of a due group

```text
candidates = _locks.find({available_at: {$lte: now}}).sort(available_at, 1).limit(8)   # only the due ones
for c in candidates:
    token = "{worker_id}:{uuid}"
    leased = _locks.find_one_and_update(
        {_id: c._id, available_at: {$lte: now}},               # re-check in the filter: only one wins
        {$set: {token: token, available_at: now + visibility}})
    if leased is None:  continue                                # another worker leased it first
    head = _messages.find_one({group_id: c._id}, sort=[(_id, 1)])   # the oldest message, NOT removed
    if head is None:  _locks.delete_one({_id: c._id, token: token}); continue   # stale group, release
    return Lease(head._id, group_id, head.event, token=token)
```

The lease is one **atomic `find_one_and_update`** whose filter still requires `available_at <=
now`: two workers racing the same candidate — only one matches and wins, the loser's filter no
longer matches once the winner bumps `available_at` out by `visibility`. That bump also makes the
group reappear once the lease expires (crash recovery, no sweep). The head is returned, not
removed.

## ack / nack — fenced by the token

```text
ack(lease):   if _locks doc's token != lease.token: return            # fencing
              _messages.delete_one({_id: lease.seq})                   # remove the head
              if more messages in group:
                 _locks.update_one({_id:G, token:lease.token}, {$set:{available_at:0.0, token:None}})  # FIFO: next now
              else:
                 _locks.delete_one({_id:G, token:lease.token})         # group drained -> drop it

nack(lease, delay):  (token-fenced)
   if delay>0:  $set available_at = now+delay        # park (keep token so the head isn't re-claimed)
   else:        $set available_at = 0.0, token = None # retry now
```

Every mutation is fenced on the current `token` (`_owns`), so an expired-lease worker can't
disturb a group another worker has taken. `nack(delay>0)` parks the group until the delay passes.

## FIFO

The head of a group is the smallest `_id`; one consumer per group at a time → order preserved.

## Async twin

`AsyncMongoTransport` mirrors this over `motor.motor_asyncio` — awaited, the same per-group lock
document + atomic `find_one_and_update` lease.

## When to pick it

A no-Redis distributed queue for document-store shops; the per-group ready-index keeps `claim`
cheap under a backlog. Unify with [MongoStore](../stores/mongo) for an all-Mongo stack. See the
[transports hub](../transports) and [distribution](../distribution).
