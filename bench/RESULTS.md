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
| SurrealDB  | 136 | ~420 | 444 | stable from c≥8 (\*) |
| rqlite     | 69  | 158 | 147 | slowest — Raft consensus + HTTP + fsync per write (distributed-durability cost) |

(\*) One run produced an anomalous 13 ev/s at Surreal c=16 (a one-off connection/warmup
stall, 30s); the re-run gave the real curve (~420 stable). Recorded for honesty.

Backend versions (Docker): redis:7-alpine, postgres:16-alpine, rqlite/rqlite:8.43.4,
mongo:7, surrealdb/surrealdb:v2.1.4.

### Reading
- **Every backend scales with concurrency** (2–3× from c=1 to c≥16): neither the claim
  nor the commit serializes anymore (validates the ZSET-claim and Postgres-pool fixes,
  and the Mongo/Surreal O(N)-claim fix).
- Ranking: Redis ≳ Postgres > Mongo > SurrealDB ≫ rqlite — consistent with each backend's
  durability model.
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
**per-event protocol** (load + rewrite the whole Execution JSON + separate transport claim/ack +
outbox + dedupe each event) vs DBOS's leaner step checkpoint, and (b) head-of-queue contention
that DBOS only beats by **partitioning the queue** (a future per-partition queue here). Part of
the gap is also inherent to being a full hierarchical statechart (richer per-transition work
than a linear durable function).

## Per-event round-trips — folding the dedupe into the load

The per-event protocol is `claim → load → is_processed → commit → ack` — ~5 round-trips, two of
them store reads (`load` then a separate `is_processed` dedupe check). On real backends the
system is **IO-bound** (it runs at ~1k ev/s, far below the in-memory CPU ceiling of ~47k ev/s),
so cutting a round-trip per event is a direct win.

`store.load_for_event(execution_id, event_id) -> (Execution, processed)` returns both in **one**
round-trip (the worker prefers it, falling back to `load` + `is_processed` for any store that
lacks it). Implemented across all networked async stores: Postgres (`SELECT data, EXISTS(...)`),
Redis (pipelined `GET` + `SISMEMBER`), Rqlite (one HTTP `SELECT` + `EXISTS` subquery), SQLite,
SurrealDB (a `processed` subquery in the `SELECT`), Mongo (an aggregation with a server-side
`$in` so the growing `processed` array is never shipped), DynamoDB (`BatchGetItem` across the
two tables).

Controlled A/B on **real Postgres** (store+transport both PG, fresh container, 2 runs each,
no-op action), aggregate events/s:

| workers | load + is_processed (before) | load_for_event (after) | gain |
|---:|---:|---:|---:|
| 1 | ~578 | ~640 | **+11%** |
| 4 | ~842 | ~903 | **+7%** |
| 8 | ~852 | ~957 | **+12%** |

A consistent ~7–12% from removing one of ~5 round-trips. Modest but real, and free for every
networked backend. The dominant remaining per-event cost is the `commit` (the full-Execution
snapshot write + outbox + transport ack), which is the next thing to attack.

## Methodology notes
- Setup (create executions + publish the backlog) is **not** measured; only the drain is.
- `--no-sleep` isolates backend cost. With the default 10 ms async action, all backends
  converge near ~the action-bound rate and the differences wash out — use `--no-sleep`
  to compare backends.
- DynamoDB/SQS not included: they need LocalStack (an AWS *simulation*), excluded here on
  purpose; available on request (caveat: LocalStack latency ≠ real AWS).
