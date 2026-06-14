# RedisTransport — pure-Redis, no global lock

Redis has no native message groups, so `RedisTransport` builds per-group exclusivity by hand —
but without a global lock, so workers claim *different* groups in parallel. Three key families
(under `prefix`), and the design goal is that `claim` costs `O(log N + K)` in the number of
pending groups, never a scan of the whole queue. The client is injected (any redis-py-compatible
client, e.g. fakeredis), so `redis` is an optional extra; it pairs with [`RedisStore`](../stores/redis)
for a pure-Redis stack.

## Key space

```text
{prefix}:q:{group_id}   list   — the per-group FIFO (RPUSH to enqueue; index 0 = oldest)
{prefix}:lock:{group_id} string (SET NX PX) — the group lock AND the fencing token; its TTL = the lease
{prefix}:ready          ZSET   — groups that have messages, scored by epoch-ms when next claimable (0 = now)
```

- **`q:{G}`** is the FIFO: `RPUSH` appends, the head (`LINDEX 0`) is the oldest message.
- **`lock:{G}`** is set with `SET NX PX`: only one worker can hold it (synchronous mutual
  exclusion that makes the claim race-safe), and the `PX` TTL — the visibility window — **auto-
  releases it if the worker dies**, so the head becomes claimable again with no separate sweep.
  Its value is a unique `worker_id:uuid` **fencing token** (so only the current holder may
  ack/nack).
- **`ready`** is the index: a sorted set of group ids scored by the epoch-ms at which each group
  is next claimable. `claim` reads only the few lowest-scored due groups
  (`ZRANGEBYSCORE -inf now LIMIT 0 K`, `_CANDIDATES = 8`), so its cost is `O(log N + K)` — not the
  old `SMEMBERS` over every group, which collapsed throughput under a large backlog.

## publish

```text
RPUSH q:{G} event_json
ZADD ready {G: 0} NX        # NX: never reset the score of an already-scheduled group
```

The `NX` is important: a publish into a group that is **in flight or parked** must not pull its
`ready` score back to 0 and make it claimable before its lease/park elapses. A brand-new group
gets score 0 (claimable now).

## claim — lock a due group, return its head

```text
candidates = ZRANGEBYSCORE ready -inf now LIMIT 0 8      # only the few due groups
for G in candidates:
    token = "{worker_id}:{uuid}"
    if not SET lock:{G} token NX PX=visibility:  continue   # someone else holds it -> next candidate
    payload = LINDEX q:{G} 0                                # the head, NOT removed
    if payload is None:                                     # stale group, no messages
        ZREM ready {G};  DEL lock:{G};  continue
    ZADD ready {G: now + visibility}                        # bump out of the due window
    return Lease(seq=0, group_id=G, event=payload, token=token)
```

`SET … NX PX` is the per-group lock: exactly one worker wins a candidate, and the lock expires on
its own (the lease) if the worker dies. Bumping the `ready` score out by `visibility` means other
claimers skip the group **and** it reappears as a candidate once the lease expires — recovery
without a sweeper. The head is returned but **not removed** (it's removed on `ack`).

## ack / nack — fenced by the token

```text
ack(lease):   if GET lock:{G} != token: return            # fencing: only the holder proceeds
              LPOP q:{G}                                    # remove the head
              if LLEN q:{G} == 0:  ZREM ready {G}           # group drained -> drop it
              else:                ZADD ready {G: 0}        # next message claimable now (FIFO)
              DEL lock:{G}                                  # free the group

nack(lease, delay):  if GET lock:{G} != token: return
              if delay>0:   ZADD ready {G: now+delay};  SET lock:{G} token PX=delay   # park (keep lock)
              else:         ZADD ready {G: 0};          DEL lock:{G}                  # retry now
```

Every mutation is **fenced** on still owning the lock (`_owns`): a worker whose lease expired
mid-handling can't corrupt a group another worker has since taken (the store's version/CAS is the
deeper backstop). `nack(delay>0)` **parks**: it pushes the `ready` score into the future *and*
keeps the lock for the same window, so the still-present head isn't re-claimed until the park
elapses — the control-plane's suspended-group park.

## FIFO

Within a group, `q:{G}` is a list consumed head-first and only one worker holds the lock at a
time, so order is preserved; on `ack` the group is re-readied at score 0 so its next message is
immediately claimable.

## Async twin

`AsyncRedisTransport` mirrors this over `redis.asyncio` (awaited pipeline; same `SET NX PX` lock,
`ready` ZSET and fencing).

## When to pick it

Fast, all-network, no global lock — workers lease different groups concurrently. Pairs with
[RedisStore](../stores/redis) for a pure-Redis deployment. See the [transports hub](../transports)
and [distribution](../distribution).
