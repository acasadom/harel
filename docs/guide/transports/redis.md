# RedisTransport — pure-Redis, no global lock

Redis has no native message groups, so `RedisTransport` builds per-group exclusivity by hand —
but without a global lock, so workers claim *different* groups in parallel. Three key families
(under `prefix`), and the design goal is that `claim` costs `O(log N + K)` in the number of
pending groups, never a scan of the whole queue. Both `claim` and `ack` run as **atomic
server-side Lua scripts** (`_CLAIM_LUA` / `_ACK_LUA`), so each is one round-trip and concurrent
claimers never race for the same group. The client is injected (any redis-py-compatible client,
e.g. fakeredis), so `redis` is an optional extra; it pairs with [`RedisStore`](../stores/redis)
for a pure-Redis stack. (The Lua scripts need `lupa` installed for the fakeredis-backed unit
tests; a real Redis runs Lua natively.)

## Key space

```text
{prefix}:q:{group_id}   list   — the per-group FIFO (RPUSH to enqueue; index 0 = oldest)
{prefix}:lock:{group_id} string — the group lock AND the fencing token; its PX TTL = the lease
{prefix}:ready          ZSET   — groups that have messages, scored by epoch-ms when next claimable (0 = now)
```

- **`q:{G}`** is the FIFO: `RPUSH` appends, the head (`LINDEX 0`) is the oldest message.
- **`lock:{G}`** is the group lock *and* the fencing token: only one worker can hold it, and its
  `PX` TTL — the visibility window — **auto-releases it if the worker dies**, so the head becomes
  claimable again with no separate sweep. Its value is a unique `worker_id:uuid` **fencing token**
  (so only the current holder may ack/nack). It is now `SET` **inside the atomic `claim` Lua
  script**, not a client-side `SET NX` — so two workers never race for the same group.
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

## claim — one atomic Lua: lock a due group, return its head

`claim` is a single server-side Lua script (`_CLAIM_LUA`) run in ONE round-trip. It scans the
lowest-scored due groups, locks the first whose lock is free and whose queue has a head, bumps that
group's `ready` score out of the visibility window, and returns its head — all atomically:

```text
claim(token, visibility):           # ONE atomic Lua round-trip, KEYS=[ready], ARGV=[prefix, now, px, token, K]
    cands = ZRANGEBYSCORE ready -inf now LIMIT 0 K       # only the few due groups (K = _CANDIDATES = 8)
    for G in cands:
        if EXISTS lock:{G} == 0:                         # lock free?
            payload = LINDEX q:{G} 0                     # the head, NOT removed
            if payload:
                SET lock:{G} token PX=visibility         # take the lease (inside the script, no SET NX race)
                ZADD ready {G: now + visibility}         # bump out of the due window
                return {G, payload}
            else:
                ZREM ready {G}                           # stale empty group -> drop it
    return nil
```

Because the whole scan-lock-bump is **one atomic script**, concurrent claimers each get a
**distinct** group — there is no client-side `SET NX` loop and so no lost-lock races (the old
client-side version had workers race for the same candidate head and burn round-trips on lost
`SET NX` locks, plateauing throughput around 8 workers and regressing beyond). The lock still
expires on its own (the lease) if the worker dies; bumping the `ready` score out by `visibility`
means other claimers skip the group **and** it reappears as a candidate once the lease expires —
recovery without a sweeper. The head is returned but **not removed** (it's removed on `ack`).

## ack — one atomic Lua; nack — fenced by the token

`ack` is also a single server-side Lua script (`_ACK_LUA`) in ONE round-trip: it fences on the lock
token, pops the delivered head, re-readies the group (or drops it if now empty), and frees the
lock. `nack` stays a small client-side pair of writes, fenced on the token:

```text
ack(lease):           # ONE atomic Lua round-trip, KEYS=[ready], ARGV=[prefix, G, token]
    if GET lock:{G} != token: return 0                # fencing: only the current holder proceeds
    LPOP q:{G}                                         # remove the head
    if LLEN q:{G} == 0:  ZREM ready {G}                # group drained -> drop it
    else:                ZADD ready {G: 0}             # next message claimable now (FIFO)
    DEL lock:{G}                                       # free the group

nack(lease, delay):  if GET lock:{G} != token: return
              if delay>0:   ZADD ready {G: now+delay};  SET lock:{G} token PX=delay   # park (keep lock)
              else:         ZADD ready {G: 0};          DEL lock:{G}                  # retry now
```

Folding `ack` into one Lua replaces the old GET+LPOP+LLEN+ZADD/ZREM+DEL (~5 round-trips) **and**
closes the lock-expires-mid-ack window the multi-command version had. Every mutation is **fenced**
on still owning the lock: a worker whose lease expired mid-handling can't corrupt a group another
worker has since taken (the store's version/CAS is the deeper backstop). `nack(delay>0)` **parks**:
it pushes the `ready` score into the future *and* keeps the lock for the same window, so the
still-present head isn't re-claimed until the park elapses — the control-plane's suspended-group
park.

## FIFO

Within a group, `q:{G}` is a list consumed head-first and only one worker holds the lock at a
time, so order is preserved; on `ack` the group is re-readied at score 0 so its next message is
immediately claimable.

## Async twin

`AsyncRedisTransport` mirrors this over `redis.asyncio` (awaited; the same atomic `_CLAIM_LUA` /
`_ACK_LUA` scripts, `lock:` lease, `ready` ZSET and fencing).

## When to pick it

Fast, all-network, no global lock — workers lease different groups concurrently. Pairs with
[RedisStore](../stores/redis) for a pure-Redis deployment. See the [transports hub](../transports)
and [distribution](../distribution).
