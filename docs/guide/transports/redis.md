# RedisTransport — pure-Redis, no global lock

Redis has no native message groups, so `RedisTransport` builds per-group exclusivity by hand —
but without a global lock, so workers claim *different* groups in parallel. Three key families
(under `prefix`), and the design goal is that `claim` costs `O(log N + K)` per priority tier (at
most 5 tiers), never a scan of the whole queue. `publish`, `claim` and `ack` all run as **atomic
server-side Lua scripts** (`_PUBLISH_LUA` / `_CLAIM_LUA` / `_ACK_LUA`), so each is one round-trip
and concurrent claimers never race for the same group. The client is injected (any redis-py-
compatible client, e.g. fakeredis), so `redis` is an optional extra; it pairs with
[`RedisStore`](../stores/redis) for a pure-Redis stack. (The Lua scripts need `lupa` installed for
the fakeredis-backed unit tests; a real Redis runs Lua natively.)

## Key space

```text
{prefix}:q:{group_id}    list   — the per-group FIFO (RPUSH to enqueue; index 0 = oldest)
{prefix}:lock:{group_id} string — the group lock AND fencing token; PX TTL = the lease
{prefix}:ready:{prio}    ZSET   — one per priority level (0-4): its groups, scored by epoch-ms
                                  when each is next claimable
{prefix}:prio            hash   — group_id → priority (int 0-4); set on first publish
```

- **`q:{G}`** is the FIFO: `RPUSH` appends, the head (`LINDEX 0`) is the oldest message.
- **`lock:{G}`** is the group lock *and* the fencing token: only one worker can hold it, and its
  `PX` TTL — the visibility window — **auto-releases it if the worker dies**, so the head becomes
  claimable again with no separate sweep. It is `SET` **inside the atomic `claim` Lua script**,
  not a client-side `SET NX` — so two workers never race for the same group.
- **`ready:{prio}`** is the index, split into **one ZSET per priority level (0-4)**: within a tier,
  group ids are scored by the epoch-ms at which each becomes claimable. `claim(min_priority=m)`
  reads the few lowest-scored due groups (`ZRANGEBYSCORE -inf now LIMIT 0 K`, `_CANDIDATES = 8`)
  **from each tier `t >= m`** and takes the globally lowest-scored (oldest-serviced) lockable one.
  So a high-priority group is served even behind a large backlog of lower-priority groups — a single
  shared ZSET filtered *inside* the candidate window would hide it (a group outside the K lowest
  scores would be starved until the backlog drained). New groups start at score 0; after `ack` the
  score is `now_ms` — the round-robin mechanism, applied within a tier.
- **`prio`** is a hash of `group_id → priority`. `HSETNX` sets it on first publish only (first
  publish wins); `claim` uses the tier, `ack`/`nack` read it to find the group's tier.

## publish — one atomic Lua: push, fix priority, ready in the tier

`publish` is a server-side Lua script (`_PUBLISH_LUA`): it pushes the payload, fixes the group's
priority on the **first** publish (`HSETNX`, clamped to 0-4), and readies the group in the ZSET of
its priority tier.

```text
publish(G, event_json, priority):   # ONE atomic Lua round-trip
    RPUSH q:{G} event_json
    HSETNX prio {G} priority              # first publish wins (clamped to 0-4)
    eff = HGET prio {G}                    # the effective (first-publish) priority
    ZADD ready:{eff} {G: 0} NX             # ready in ITS tier; NX so an in-flight/parked group isn't reset
```

The `NX` on the ZADD matters: a publish into a group that is **in flight or parked** must not pull
its score back to 0 and make it claimable before its lease/park elapses. Doing it in one Lua also
fixes the tier atomically — a race on the first publish can't split a group across two tiers.

## claim — one atomic Lua: oldest-serviced eligible group across tiers

`claim` is a single server-side Lua script (`_CLAIM_LUA`) in ONE round-trip. For `min_priority=m`
it scans the candidate window of each tier `t >= m` and takes the globally lowest-scored
(oldest-serviced) lockable group with a head:

```text
claim(token, visibility, min_priority):   # ONE atomic Lua round-trip
    # ARGV=[prefix, now, px, token, K, min_prio]; the tier/lock/queue keys are computed from prefix
    best = nil
    for t = min_prio, 4:                                    # only tiers at/above the floor
        cands = ZRANGEBYSCORE ready:{t} -inf now LIMIT 0 K  # the few due groups in this tier (K=8)
        for G in cands:
            if EXISTS lock:{G} == 0:                        # lock free?
                payload = LINDEX q:{G} 0                    # the head, NOT removed
                if payload:
                    if best == nil or ZSCORE ready:{t} {G} < best.score:
                        best = {G, payload, score, tier=t}
                    break                                   # this tier's oldest lockable; compare across tiers
                else:
                    ZREM ready:{t} {G}                      # stale empty group -> drop it
    if best:
        SET lock:{best.G} token PX=visibility               # take the lease (atomic, no SET NX race)
        ZADD ready:{best.tier} {best.G: now + visibility}   # bump out of the due window
        return {best.G, best.payload}
    return nil
```

`min_priority=0` spans every tier → the globally oldest-serviced group, i.e. plain round-robin with
all groups equal (the default when the worker's `high_ratio=0`). `min_priority>0` restricts to the
high tiers and — unlike a single shared ZSET filtered inside a fixed window — **always finds an
eligible group if one exists, regardless of the backlog size** (matching the SQL/Mongo full-scan
claims). Within a tier all groups share a priority, so bounding the candidate window to K is pure
round-robin. Because the whole scan-lock-bump is one atomic script, concurrent claimers each get a
**distinct** group.

## ack — one atomic Lua; nack — fenced by the token

`ack` is also a single server-side Lua script (`_ACK_LUA`) in ONE round-trip: it fences on the lock
token, pops the delivered head, re-readies the group **in its tier** at `now_ms` (round-robin) or
drops it, and frees the lock. `nack` stays a small client-side pair of writes, fenced on the token:

```text
ack(lease):    # ONE atomic Lua round-trip, ARGV=[prefix, G, token, now_ms]
    if GET lock:{G} != token: return 0                 # fencing: only the current holder proceeds
    LPOP q:{G}                                          # remove the head
    eff = HGET prio {G}                                 # the group's tier
    if LLEN q:{G} == 0:  ZREM ready:{eff} {G}           # group drained -> drop it from its tier
                         HDEL prio {G}                  # clean up priority (correct priority on recycle)
    else:                ZADD ready:{eff} {G: now_ms}    # round-robin: score = now, in its tier
    DEL lock:{G}                                        # free the group

nack(lease, delay):  if GET lock:{G} != token: return   # eff = HGET prio {G} (the group's tier)
              if delay>0:   ZADD ready:{eff} {G: now+delay};  SET lock:{G} token PX=delay   # park
              else:         ZADD ready:{eff} {G: 0};          DEL lock:{G}                  # retry now
```

Setting the ack score to `now_ms` (not 0) is the round-robin mechanism: a group that was just
processed sits behind newly-arriving groups (score 0) *within its tier* until those are claimed.
Folding `ack` into one Lua replaces the old GET+LPOP+LLEN+ZADD/ZREM+DEL (~5 round-trips) **and**
closes the lock-expires-mid-ack window. `nack(delay>0)` **parks**: pushes the score into the future
*and* keeps the lock so the still-present head isn't re-claimed until the park elapses.

## FIFO

Within a group, `q:{G}` is a list consumed head-first and only one worker holds the lock at a
time, so order is preserved.

## Async twin

`AsyncRedisTransport` mirrors this over `redis.asyncio` (awaited; the same atomic `_PUBLISH_LUA` /
`_CLAIM_LUA` / `_ACK_LUA` scripts, `lock:` lease, per-tier `ready:{prio}` ZSETs and fencing).

## When to pick it

Fast, all-network, no global lock — workers lease different groups concurrently. Pairs with
[RedisStore](../stores/redis) for a pure-Redis deployment. See the [transports hub](../transports)
and [distribution](../distribution).
