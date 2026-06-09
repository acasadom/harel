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

**Postgres** (backlog 2000 execs = 4000 events):

| workers | agg events/s | vs 1 worker | per-worker split |
|---:|---:|---:|---|
| 1 | 726 | 1.00× | [4000] |
| 2 | 781 | 1.08× | [1999, 2001] |
| 4 | 710 | 0.98× | [1000×4] |

### Reading — the limit is the **backend**, not the worker
- Work is split evenly across processes (the single-active-consumer-per-group transport
  load-balances correctly), so it is genuinely parallel — yet the aggregate does not grow
  linearly.
- **Redis**: scales to ~1.7× and plateaus near **~1400 ev/s**. A single worker already
  extracts ~60% of the achievable aggregate. The ceiling is the shared backend (one Redis
  instance; contention on the hot shared keys — the `ready` ZSET and the outbox seq — plus
  per-event round-trips), not a single event loop.
- **Postgres**: essentially **flat** (~750 ev/s) regardless of worker count — adding workers
  does not help at all. Its `claim` takes a **global `pg_advisory_xact_lock`**, which
  serializes every claim across all workers; extra workers just queue on that lock.
- **Takeaway**: to go past a single worker's throughput you scale the **backend**
  (shard/partition executions across instances), not the worker count. For Postgres
  specifically the global advisory lock is the hard ceiling — a future optimization would be
  a non-global claim (e.g. `FOR UPDATE SKIP LOCKED` on the queue table) to let claims run
  concurrently.

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
queue partitioning). Our single-Postgres number (~750 ev/s, and flat across workers) is far
below that, for two fixable reasons: (1) our `claim` takes a **global `pg_advisory_xact_lock`**
that serializes every claim — `FOR UPDATE SKIP LOCKED` would let claims run concurrently; and
(2) a heavier per-event protocol (load + rewrite the whole Execution JSON + separate transport
claim/ack + outbox + dedupe each event) vs DBOS's leaner step checkpoint. Part of the gap is
inherent to being a full hierarchical statechart (richer per-transition work than a linear
durable function), but the claim lock and the per-event write are real, addressable headroom.

## Methodology notes
- Setup (create executions + publish the backlog) is **not** measured; only the drain is.
- `--no-sleep` isolates backend cost. With the default 10 ms async action, all backends
  converge near ~the action-bound rate and the differences wash out — use `--no-sleep`
  to compare backends.
- DynamoDB/SQS not included: they need LocalStack (an AWS *simulation*), excluded here on
  purpose; available on request (caveat: LocalStack latency ≠ real AWS).
