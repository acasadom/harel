# MongoTransport — per-group ready-index document

A multi-machine queue on MongoDB (the document-store sibling of the SQL queues), no Redis. Like
[`RedisTransport`](redis), MongoDB has no native message groups, so per-group exclusivity is built
by hand with a **per-group lock document** that is the ready-index and the lease in one. `claim`
picks-and-leases the lowest-`available_at` due group in **one atomic sorted `find_one_and_update`**,
so its cost is `O(log N)` in the number of active groups — never a `$group` aggregation over every
message. The client is injected (`pymongo` optional; tests use mongomock); pairs with
[`MongoStore`](../stores/mongo) for an all-Mongo stack.

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
  `worker_id:uuid` fencing token). An index on `available_at` makes `claim` `O(log N)` — the
  server sorts by it and picks-and-leases the head in one atomic update.

## publish

```text
_messages.insert_one({ _id: next_seq(), group_id, event: event_json })
_locks.update_one({_id: group_id}, {$setOnInsert: {available_at: 0.0, token: None}}, upsert=True)
```

`$setOnInsert` readies the group **only if new** (a brand-new group is claimable now, score 0); a
publish into an in-flight or parked group must not pull its `available_at` back and make it
claimable before its lease/park elapses (the Mongo analogue of Redis `ZADD … NX`).

## claim — one atomic sorted lease of the due group

`claim` is a single atomic `find_one_and_update` with a `sort` — the server finds the
lowest-`available_at` due group **and** leases it in one operation; no candidate window, no
client-side loop over candidates:

```text
loop:
    token = "{worker_id}:{uuid}"
    leased = _locks.find_one_and_update(
        {available_at: {$lte: now}},                           # any group due now
        {$set: {token: token, available_at: now + visibility}}, # lease it (bump out of range)
        sort=[(available_at, 1)])                               # server picks the lowest-due one
    if leased is None:  return None                             # nothing due
    G = leased._id
    head = _messages.find_one({group_id: G}, sort=[(_id, 1)])   # the oldest message, NOT removed
    if head is None:  _locks.delete_one({_id: G, token: token}); continue   # stale empty group, drop + retry
    return Lease(head._id, G, head.event, token=token)
```

The pick-and-lease is one **atomic sorted `find_one_and_update`**: the server selects the
lowest-`available_at` group and bumps its `available_at` out by `visibility` in the same operation,
so concurrent claimers each get a **distinct** group with no lost-lease races. (The old design did
a `find().sort().limit(K)` over a candidate window then a loop of `find_one_and_update` per
candidate, where workers fished the same window and burned round-trips on lost leases — the
`_CANDIDATES` window constant is gone.) The lease itself was always atomic; the change removes the
find()+loop. The `available_at` bump also makes the group reappear once the lease expires (crash
recovery, no sweep). A stale empty group is dropped (`delete_one`) and the claim retries. The head
is returned, not removed.

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
document + one atomic sorted `find_one_and_update` lease.

## When to pick it

A no-Redis distributed queue for document-store shops; the per-group ready-index keeps `claim`
cheap under a backlog. Unify with [MongoStore](../stores/mongo) for an all-Mongo stack. See the
[transports hub](../transports) and [distribution](../distribution).
