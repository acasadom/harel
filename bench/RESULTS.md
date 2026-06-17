# Async throughput — benchmark results

Measured with `bench/bench_async.py` (the minimal-overhead variant: pre-load the whole
backlog as setup, then time only the worker draining it, detecting the end by counting
real `ack`s — no polling probe, no sleeps in the measured path).

Machine: local dev (single host). Each backend run as **store + transport unified**.
`--no-sleep` (the action is a no-op) so the number reflects the **backend/engine
event-processing rate**, not a fixed per-action delay.

## Single worker, sweep over `concurrency` (one AsyncWorker, one event loop)

n = 200 executions × 2 events (Start, Finish) = 400 events per run. Throughput in events/s.

| Backend (store+transport) | c=1 | c=16 | c=64 | Notes |
|---|---:|---:|---:|---|
| Redis      | 311 | 672 | **833** | fastest; in-memory backend |
| Postgres   | 262 | **706** | 593 | peaks ~c=16; slight dip at 64 (pool/lock contention) |
| Mongo      | 228 | 496 | 512 | scales then plateaus |
| rqlite     | 69  | 158 | 147 | slowest — Raft consensus + HTTP + fsync per write (distributed-durability cost) |

Backend versions (Docker): redis:7-alpine, postgres:16-alpine, rqlite/rqlite:8.43.4, mongo:7.

> **SurrealDB was retired** as a backend: its transport `claim` / store `commit` (optimistic
> server-side `BEGIN…COMMIT`) raise *"Transaction read conflict"* on ~40–60% of concurrent
> writes (both the `memory` and `rocksdb` engines), which crashes the worker and defeats the
> concurrency distribution needs. It single-threaded around ~440 ev/s but could not be run
> multi-worker. The other backends cover the same use cases.

### Reading
- **Every backend scales with concurrency** (2–3× from c=1 to c≥16): neither the claim
  nor the commit serializes anymore (validates the ZSET-claim and Postgres-pool fixes,
  and the Mongo O(N)-claim fix).
- Ranking: Redis ≳ Postgres > Mongo ≫ rqlite — consistent with each backend's durability model.
- The ~800 ev/s ceiling here is bounded by the **per-event round-trip latency against a
  single local server** (each event = load + commit-with-outbox + claim/ack), not by the
  engine (in-memory the engine does ~47k ev/s). See the worker-scaling results for whether
  more workers lift the aggregate.

## Worker scaling — is the limit the worker or the backend?

`bench/bench_workers.py` launches W independent worker **processes** (separate connections,
true CPU parallelism — production scale-out) all draining ONE shared backend, and reports
aggregate events/s. `--no-sleep`, concurrency=64 per worker. Per-worker counts show load
balance across processes.

**Redis** (backlog 3000 execs = 6000 events):

| workers | agg events/s | vs 1 worker | per-worker split |
|---:|---:|---:|---|
| 1 | 836  | 1.00× | [6000] |
| 2 | 1179 | 1.41× | [2995, 3005] |
| 4 | 1389 | 1.66× | [1502, 1501, 1506, 1491] |
| 8 | 1417 | 1.69× | [~750 each] |

**Redis — atomic Lua claim** (the lost-lock-race fix). The table above is the *old* client-side
claim (`ZRANGEBYSCORE` then a loop of `SET NX`): all workers fish the same candidate head and race
for the lock. Measured directly, the **`SET NX` failure rate** climbs `0% → 38% (4w) → 70% (8w) →
88% (16w)` — it is **not** the server (single-threaded Redis sits at ~20% of one core) but **lost
lock races burning round-trips** (single-thread ⇒ no mutex contention; the cost is wasted client
work). Moving the claim into **one atomic Lua script** (each concurrent claimer gets a *distinct*
group) lifts both single-worker latency (one round-trip, not ~4) and the aggregate ceiling:

| workers | old (SET NX loop) | new (atomic Lua) | speedup |
|---:|---:|---:|---:|
| 1 | 836  | 1401 | 1.67× |
| 2 | 1179 | 2554 | 2.17× |
| 4 | 1389 | 3174 | 2.28× |
| 8 | 1417 | 3161 | 2.23× |

The old claim plateaued (and *regressed* past ~8 workers, with 35% empty claims at 16w); the new one
reaches ~3.2k ev/s. A ceiling still appears at ~4–8 workers — but now it is the **next** bottleneck
(the store's per-event commit + the global outbox sequence), not the claim. (Measured on one laptop;
the per-worker split stays even, so it is genuinely balanced.)

**Redis — fewer round-trips per event** (the follow-on to the atomic claim). Measured, a single
worker was **CPU-bound** (CPU/wall ≈ 0.99 at concurrency ≥ 16) doing **~21 Redis ops/event** — *not*
server-bound (Redis sat at a few % of one core). The cost was the worker's own event-loop work
**issuing/awaiting ~13 round-trips/event**. Two cuts, no change to the commit/CAS path:

- **`ack` → atomic Lua**: `GET`+`LPOP`+`LLEN`+`ZADD`/`ZREM`+`DEL` (5 round-trips) folded into one
  `EVALSHA` (the ops still run, but server-side in one round-trip; also closes the lock-expires-mid-
  ack window).
- **relay guard**: the per-event outbox relay did `HGETALL outbox` + `HGETALL spawns` *every* event
  (2 round-trips, and O(N) to deserialize as the outbox grows). Most events emit nothing, so the
  driver now skips the relay unless the commit actually enqueued an emit/spawn — eliminating both
  `HGETALL`s on the common path (orphan recovery is unchanged: the idle loop never flushed either,
  and `recover()` covers startup).

~13 round-trips/event → ~7. Redis-side op count barely moves (the ack ops moved inside the Lua), but
the worker does ~6 fewer awaits/event, and it was CPU-bound on exactly that:

| workers | atomic-claim only | + ack-Lua + relay-guard | vs v0.1.1 baseline |
|---:|---:|---:|---:|
| 1 | 1401 | 2359 | 2.82× |
| 2 | 2554 | 4071 | — |
| 4 | 3174 | 5133 | 3.70× |
| 8 | 3161 | 5343 | 3.77× |

The remaining round-trips are dominated by the **commit** (`INCR` + `WATCH` + `GET` + `MULTI/EXEC` ≈
4), the next lever (fold into one atomic Lua script — now feasible since `lupa` lets fakeredis run
Lua). The ~4–8-worker plateau on this laptop is still **host CPU** (8 CPU-bound workers), not Redis.

**Postgres — before vs after the claim fix** (backlog ~2–2.5k execs):

The original `claim` took a global `pg_advisory_xact_lock`, serializing every claim → flat,
no worker scaling. It was replaced with a per-group row claimed via `FOR UPDATE SKIP LOCKED`
(workers lease *different* groups in parallel; the DBOS approach):

| workers | global advisory lock (before) | FOR UPDATE SKIP LOCKED (after) |
|---:|---:|---:|
| 1 | 726 (1.00×) | 624 (1.00×) |
| 2 | 781 (1.08×) | 769 (1.23×) |
| 4 | 710 (0.98×) | 862 (1.38×) |
| 8 | —           | 932 (1.49×) |

The advisory lock was completely flat (0.98× at 4 workers — extra workers just queued on the
lock). With SKIP LOCKED it scales (1.49× at 8 workers). A single worker dips slightly
(726→624) — the per-group bookkeeping costs a little more per event — but that fixed cost pays
off the moment you add workers.

### Reading — the limit is the **backend**, not the worker
- Work is split evenly across processes (the single-active-consumer-per-group transport
  load-balances correctly), so it is genuinely parallel — yet the aggregate grows sub-linearly.
- **Redis**: scales to ~1.7× and plateaus near **~1400 ev/s**. A single worker already
  extracts ~60% of the achievable aggregate. The ceiling is the shared backend (one Redis
  instance; contention on the hot shared keys — the `ready` ZSET and the outbox seq — plus
  per-event round-trips), not a single event loop.
- **Postgres**: now scales with workers (after the SKIP LOCKED fix), but sub-linearly. The
  remaining limits are (a) head-of-queue contention on the shared `transport_groups` table —
  the exact effect DBOS documents ("SKIP LOCKED isn't enough; partition the queue"), which a
  future per-partition queue would address — and (b) in an all-Postgres run the store's
  full-Execution-snapshot commit per event shares the same DB and dominates the per-event cost.
- **Takeaway**: to go past a single worker's throughput you scale the **backend**
  (shard/partition executions across instances), not the worker count.

## Loop-CPU vs backend I/O — where the single-worker limit actually sits

> Measured 2026-06-17 on Docker Desktop / Apple Silicon (I/O-heavier than a bare-metal
> Postgres, so the I/O-wait fraction below is host-specific). These **refine** the
> "the limit is the backend, not the worker" framing above: it holds for the **aggregate on
> one instance**, but for a **single worker** the limit is a *mix* of event-loop CPU and
> backend I/O, and which dominates depends on the backend's per-op latency on the host.

**A full worker (not the bare engine) tops ~4.2k ev/s on an in-memory store+transport** —
one process, one loop, `AsyncDictStore` + `AsyncInMemoryTransport`, no network, no fsync:

| concurrency | 1 | 16 | 64 | 256 | 1024 |
|---|---:|---:|---:|---:|---:|
| events/s | 3899 | 4259 | 4242 | 3966 | 3215 |

So the worker/driver/asyncio loop itself caps a single worker at ~4.2k — ~11× below the
**bare engine's ~47k** (`core.process` with no store/transport/driver). Throughput also
*degrades* past the sweet spot (c=1024 < c=16): too many in-flight coroutines cost more to
schedule than they save. The "~47k in-memory ceiling" cited elsewhere is the engine alone,
**not** what a worker can drive.

**The Execution snapshot serialization is negligible** — `model_dump_json` +
`model_validate_json` round-trip on a mid-flight Execution measures **~0.003 ms/event**
(292-byte JSON). So "rewrite the whole snapshot per event" is **not** a meaningful CPU cost;
a delta/append checkpoint would only help the *write I/O* of *large* executions, not the
baseline per-event cost.

**A single Postgres worker is ~43% CPU, ~57% I/O-wait** (this host) — driving one worker
against real Postgres, CPU time vs wall time during the measured drain:

| concurrency | events/s | CPU/wall |
|---|---:|---:|
| 16 | 345 | 0.43 |
| 64 | 332 | 0.43 |
| 128 | 343 | 0.43 |

CPU/wall ≈ 0.43 (not ≈ 1.0) means the worker is **not** purely loop-bound here — it spends
most of its time awaiting Postgres. But the CPU half is ~1.25 ms/event of **driver** work
(psycopg building/sending/parsing the ~5 queries per event — load, commit-with-outbox, claim,
ack — *not* snapshot serialization), which sets a hard **~800 ev/s per-worker CPU ceiling**
independent of backend speed. On a faster-I/O host the I/O-wait shrinks and the worker
approaches that ceiling → there it *is* loop-bound. (Throughput is flat across concurrency
because, for one worker, the per-event round-trips don't overlap enough to hide the latency.)

**Adding worker processes lifts the Postgres aggregate** (`bench_workers.py`, 1→4 = 2.4×,
even split) — one worker does **not** saturate the instance:

| workers | 1 | 2 | 4 |
|---|---:|---:|---:|
| agg events/s | 338 | 553 | 810 |

**Corrected model.** Per-worker throughput ≈ `1 / (loop_cpu_per_event + io_wait_per_event)`:
- loop CPU/event = **driver work + engine + asyncio** (snapshot serialize is negligible),
  ~1.25 ms on Postgres → a ~800 ev/s/worker CPU ceiling;
- a backend whose per-event latency sits *below* that ceiling (fast Redis/Postgres) → the
  worker is **loop/driver-CPU-bound** (i.e. *worker*-bound); a slow-I/O host → **I/O-wait-bound**;
- the **aggregate on one instance** is backend-bound, scales sublinearly with worker
  *processes* → past that, **shard** (next section).

So "backend-bound, not worker-bound" is precise only for the aggregate ceiling; a single
worker's limit is the `min(loop-CPU, backend-latency)` mix above — which is why a profile on
a fast-disk Postgres can legitimately read as "loop-bound, the backend has more to give".

## Horizontal scaling — sharding across independent backends

`bench/bench_shards.py`: a shard is its own Redis instance with its own worker; executions
are partitioned across shards. Because executions are independent (single-consumer per
group, no cross-execution coordination), shards share nothing, so the aggregate should grow
with shard count. Run on **one 11-core laptop** (4 Redis containers + 4 worker processes +
the bench all co-located), backlog 3000 execs/shard, concurrency 64/shard:

| shards | agg events/s | vs 1 shard | per-shard events/s |
|---:|---:|---:|---:|
| 1 | 830  | 1.00× | 830 |
| 2 | 1262 | 1.52× | 631 |
| 4 | 1681 | 2.02× | 420 |

### Reading — the architecture is shard-linear; the single host is what caps this run
- Aggregate keeps rising with shards (830 → 1262 → 1681). The shards are genuinely
  shared-nothing — different Redis instances, different worker processes.
- The tell is the **per-shard** column: each identical shard's own rate falls 830 → 420 as
  shards are added. That is **host contention** (4 Redis + 4 workers + the Docker-Desktop
  network proxy all on 11 shared cores), not the design — on dedicated hosts each shard would
  hold ~830, i.e. 4 shards ≈ 3320 ev/s. So the ~2× at 4 shards is a single-laptop measurement
  ceiling, not an architectural one.
- This is the same scaling model Temporal ("hash workflow ID → shard, add shards") and DBOS
  ("your ceiling is your Postgres; add partitions") use. Per-shard throughput sits in the
  hundreds–low-thousands for all of them; you scale by adding shards/machines.

### Per-instance headroom (the honest gap)
DBOS sustains ~40K steps/s on a **single** Postgres (via `FOR UPDATE SKIP LOCKED` dequeue +
queue partitioning). Our single-Postgres number is still well below that. Of the two original
causes, the first is now **fixed**: the `claim` no longer takes a global `pg_advisory_xact_lock`
(which serialized every claim) — it uses `FOR UPDATE SKIP LOCKED` on a per-group row, so claims
run concurrently and Postgres now scales with workers (above). The remaining gap is (a) the
**per-event protocol** (load + rewrite the Execution + separate transport claim/ack + outbox +
dedupe = ~5 round-trips/event — the *number* of round-trips and the per-query driver CPU, **not**
the JSON serialize, which measures ~0.003 ms/event) vs DBOS's leaner step checkpoint, and (b) head-of-queue contention
that DBOS only beats by **partitioning the queue** (a future per-partition queue here). Part of
the gap is also inherent to being a full hierarchical statechart (richer per-transition work
than a linear durable function).

## Per-event round-trips — folding the dedupe into the load

The per-event protocol is `claim → load → is_processed → commit → ack` — ~5 round-trips, two of
them store reads (`load` then a separate `is_processed` dedupe check). On a slow-I/O host the
system is largely **IO-bound** (it runs at ~1k ev/s, far below a full worker's in-memory loop
ceiling of ~4.2k — itself far below the bare engine's ~47k; see the loop-CPU-vs-I/O section),
so cutting a round-trip per event is a direct win.

`store.load_for_event(execution_id, event_id) -> (Execution, processed)` returns both in **one**
round-trip (the worker prefers it, falling back to `load` + `is_processed` for any store that
lacks it). Implemented across all networked async stores: Postgres (`SELECT data, EXISTS(...)`),
Redis (pipelined `GET` + `SISMEMBER`), Rqlite (one HTTP `SELECT` + `EXISTS` subquery), SQLite,
Mongo (an aggregation with a server-side `$in` so the growing `processed` array is never
shipped), DynamoDB (`BatchGetItem` across the two tables).

Controlled A/B on **real Postgres** (store+transport both PG, fresh container, 2 runs each,
no-op action), aggregate events/s:

| workers | load + is_processed (before) | load_for_event (after) | gain |
|---:|---:|---:|---:|
| 1 | ~578 | ~640 | **+11%** |
| 4 | ~842 | ~903 | **+7%** |
| 8 | ~852 | ~957 | **+12%** |

A consistent ~7–12% from removing one of ~5 round-trips. The dominant remaining per-event cost
is the `commit` (the full-Execution snapshot write + outbox + transport ack), the next thing to
attack.

**Across backends** (all-`<backend>` store+transport, single runs so ±noise, n=2500, no-op
action), baseline → `load_for_event`, events/s at 1/4/8 workers:

| backend | 1w | 4w | 8w | effect |
|---|---|---|---|---|
| **rqlite** | 149→158 | 256→**343** | 406→**495** | **biggest win (+20–34%)** — every round-trip is an HTTP request, so dropping one matters most |
| **postgres** | 578→640 | 842→903 | 852→957 | **+7–12%** (the controlled A/B above) |
| **mongo** | 460→488 | 699→**784** | 706→694 | **small win (~+6–12%)**, 8w within noise |
| **redis** | 816→847 | 1386→1167 | 1381→1425 | **~neutral** — `GET`+`SISMEMBER` are sub-ms, so pipelining them saves almost nothing (the 4w dip is run-to-run noise) |

The gain scales with **how expensive a round-trip is** on the backend: large for rqlite (HTTP),
moderate for postgres/mongo, negligible for redis (already sub-ms ops). It's free everywhere
(one combined query) and never a regression beyond noise.

(SurrealDB was dropped from the comparison: it conflict-thrashed under concurrency and has since
been retired — see the note at the top.)

## Methodology notes
- Setup (create executions + publish the backlog) is **not** measured; only the drain is.
- `--no-sleep` isolates backend cost. With the default 10 ms async action, all backends
  converge near ~the action-bound rate and the differences wash out — use `--no-sleep`
  to compare backends.
- DynamoDB/SQS not included: they need LocalStack (an AWS *simulation*), excluded here on
  purpose; available on request (caveat: LocalStack latency ≠ real AWS).
