# MongoTransport — per-group ready-index document

A multi-machine queue on MongoDB (the document-store sibling of the SQL queues), no Redis. Like
[`RedisTransport`](redis), MongoDB has no native message groups, so per-group exclusivity is built
by hand with a **per-group lock document** that is the ready-index and the lease in one. `claim`
picks-and-leases the lowest-`available_at` due group in **one atomic sorted `find_one_and_update`**,
so its cost is `O(log N)` in the number of active groups — never a `$group` aggregation over every
message. The client is injected (`pymongo` optional; tests use mongomock); pairs with
[`MongoStore`](../stores/mongo) for an all-Mongo stack.

## Collections (under `db_name`, named by `prefix`, default `"harel"`)

```text
{prefix}_transport_messages  one doc per message: { _id: seq, group_id, event }
                   (seq = monotonic; oldest = smallest _id)
{prefix}_transport_locks     one doc per active group:
                   { _id: group_id, available_at: float, token: str|None, priority: int }
{prefix}_transport_counters  the monotonic seq allocator ({_id:"seq"}, $inc n)
```

- **`_transport_messages`** is the FIFO: `_id` is a monotonic seq from `_next_seq` (a `find_one_and_update`
  `$inc`), so the head of a group is the smallest `_id`.
- **`_transport_locks`** is the ready-index **and** the lock in one document per group: `available_at` is
  the epoch at which the group is next claimable (0 = never-yet-claimed; `now` after ack —
  **round-robin**; `now + visibility` while in flight; `now + delay` while parked), `token` is the
  current lease, and `priority` is the execution's priority level (0–4) fixed at first publish.
  An index on `available_at` makes `claim` `O(log N)` — the server sorts by it and picks-and-leases
  the head in one atomic update.

## publish

```text
_messages.insert_one({ _id: next_seq(), group_id, event: event_json })
_locks.update_one(
    {_id: group_id},
    {$setOnInsert: {available_at: 0.0, token: None, priority: priority}},
    upsert=True)
```

`$setOnInsert` readies the group **only if new** (score 0, claimable now); a publish into an
in-flight or parked group must not pull its `available_at` back. Priority is also written via
`$setOnInsert` — first publish wins; subsequent publishes do not overwrite it.

## claim — one atomic sorted lease with priority filtering

`claim` is a single atomic `find_one_and_update` with a `sort` — the server finds the
lowest-`available_at` due group at the given priority floor **and** leases it in one operation:

```text
loop:
    token = "{worker_id}:{uuid}"
    leased = _locks.find_one_and_update(
        {available_at: {$lte: now}, priority: {$gte: min_priority}},  # due + priority floor
        {$set: {token: token, available_at: now + visibility}},         # lease it (bump out of range)
        sort=[(available_at, 1)])                                        # server picks lowest-due one
    if leased is None:  return None                                      # nothing due at this priority
    G = leased._id
    head = _messages.find_one({group_id: G}, sort=[(_id, 1)])            # oldest message, NOT removed
    if head is None:  drop_group(G, token); continue                     # stale empty group, drop + retry
    return Lease(head._id, G, head.event, token=token)
```

The pick-and-lease is **one atomic sorted `find_one_and_update`**: concurrent claimers each get a
**distinct** group. The `sort=[(available_at, 1)]` drives round-robin: groups with `available_at=0`
(never claimed) come before groups with `available_at=now` (just processed). Passing
`min_priority=0` (the default) considers all groups regardless of priority.

## ack / nack — fenced by the token

```text
ack(lease):   if _locks doc's token != lease.token: return             # fencing
              _messages.delete_one({_id: lease.seq})                    # remove the head
              if more messages in group:
                 _locks.update_one({_id:G, token:lease.token},
                     {$set:{available_at: now, token:None}})             # round-robin: now (not 0.0)
              else:
                 drop_group(G, lease.token)                             # group drained -> drop it

drop_group(G, token):
   dropped = _locks.find_one_and_delete({_id:G, token:token})
   if dropped and a message for G exists:      # a publish landed between the check and the delete
      _locks.update_one({_id:G}, {$setOnInsert: {available_at:0, token:None,
                                                  priority: dropped.priority}}, upsert=True)

A publish writes its message, then upserts the group's lock; there is no row lock to make the
check-then-delete atomic, so `drop_group` re-checks after deleting. A publish that landed in
between — its message in, its upsert a no-op on the lock still there — is then seen, and the
group readied again, with its priority.

nack(lease, delay):  (token-fenced)
   if delay>0:  $set available_at = now+delay        # park (keep token so the head isn't re-claimed)
   else:        $set available_at = 0.0, token = None # retry now
```

Setting `available_at = now` on ack (instead of `0.0`) is the round-robin mechanism: a just-acked
group sits behind new groups (`available_at=0`) in the `sort`. Every mutation is fenced on the
current `token` (`_owns`), so an expired-lease worker can't disturb a group another worker has
taken. `nack(delay>0)` parks the group until the delay passes.

## FIFO

The head of a group is the smallest `_id`; one consumer per group at a time → order preserved.

## Async twin

`AsyncMongoTransport` mirrors this over `motor.motor_asyncio` — awaited, the same per-group lock
document + one atomic sorted `find_one_and_update` lease.

## When to pick it

A no-Redis distributed queue for document-store shops; the per-group ready-index keeps `claim`
cheap under a backlog. Unify with [MongoStore](../stores/mongo) for an all-Mongo stack. See the
[transports hub](../transports) and [distribution](../distribution).
