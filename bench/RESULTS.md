# Benchmark results

Measured on 2026-10-01 with the benchmarks in this directory, at the commit that added this file;
the SQLite rows were measured again the same day once its transport's claim stopped scanning the
backlog (see [SQLite's claim](#sqlites-claim)).

## Read this first

- **Host:** one Apple M1 laptop (8 cores) with the backends in Docker Desktop. The benchmarks
  ran in a container on the same Docker network as the backends, with no other harel worker
  running. Workers, backends and benchmark share the same 8 cores.
- **Read the numbers as relative.** Same host, same day, same method: the ratios between rows
  hold. The absolute rates are this laptop's, not the backends' capacity; a backend on its own
  hardware, with workers on other machines, goes much higher.
- **Machine:** each execution starts in `Working`, whose entry action runs on the `Start`, and
  ends on `Finish` in `Done` — two events per execution. Every run below uses `--no-sleep` (the
  action does nothing), so the rate is the engine + backend cost per event.
- **Store and transport on the same backend** in every row (SQLite: two files, one for each,
  unless the row says otherwise).
- Previous versions of this file measured a benchmark machine whose action never ran and
  whose numbers aren't comparable; they are in the git history.

## 1. One worker, sweeping concurrency

`bench_async.py --no-sleep --n-executions 1000` (2000 events, enqueue not timed). Median of 3
runs, events/s; Redis varied the most between runs, so its range is given.

| Backend | c=1 | c=16 | c=64 |
|---|---:|---:|---:|
| Redis | 1488 (1359–1593) | 2582 (2355–3600) | 2963 (1463–3278) |
| Postgres | 488 | 940 | 999 |
| SQLite | 1053 | 1101 | 1133 |
| Mongo | 321 | 493 | 491 |
| rqlite | 130 | 146 | 139 |

- Postgres and Mongo gain about 2× up to c=16 and stay there.
- rqlite barely gains: every operation is an HTTP request through Raft consensus.
- SQLite gains little: one writer at a time per file.

### SQLite: separate files or one

The same run with the store and the transport in **one** file:

| SQLite | c=1 | c=16 | c=64 |
|---|---:|---:|---:|
| two files | 1053 | 1101 | 1133 |
| one file | 1025 | 702 | 828 |

With one file the store's commits and the queue's claims and acks wait for the same write lock:
level with one event in flight, behind with more. Give them a file each.

### SQLite's claim

The SQLite transport's claim used to read every pending message to pick one, so it slowed as the
backlog grew. It now walks the groups in claim order on an index and stops at the first one with
nothing in flight. Same run, before and after (two files):

| One worker | c=1 | c=16 | c=64 |
|---|---:|---:|---:|
| before, 1000 executions | 705 | 758 | 765 |
| after, 1000 executions | 1053 | 1101 | 1133 |
| before, 4000 executions | 340 | 351 | 357 |
| after, 4000 executions | 975 | 1097 | 1107 |

The rate no longer depends on the backlog: 4000 executions go as fast as 1000.

## 2. Worker processes on one backend

`bench_workers.py --n-executions 2000 --concurrency 64` (4000 events; Redis: 8000 executions,
two runs). W worker processes drain a pre-loaded backlog; events/s across all of them.

| Backend | 1 w | 2 w | 4 w | 8 w | Split between workers |
|---|---:|---:|---:|---:|---|
| Redis | 3353 / 3512 | 6269 / 6656 | 8043 / 6506 | 7768 / 7559 | even |
| Postgres | 728 | 1142 | 1073 | 1122 | even |
| Mongo | 433 | 731 | 1005 | 1136 | even |
| rqlite | 117 | 197 | 237 | 298 | even |
| SQLite (two files) | 1068 | 1003 | 795 | 974 | very uneven, e.g. 8 w: [7, 165, 554, 333, 380, 1137, 68, 1356] |
| SQLite (one file) | 771 | 804 | 772 | 782 | very uneven |

- **Redis** about doubles with a second worker and levels off at 7–8k events/s from 4: 8 worker
  processes on 8 cores, shared with Redis and Docker, leave no CPU to spare.
- **Postgres** levels off at about 1100 events/s from 2 workers.
- **Mongo** scales the most in proportion: 2.6× from 1 to 8 workers.
- **rqlite** keeps scaling (2.5×): its limit is the latency of each request, which more
  workers overlap.
- **SQLite doesn't scale with processes.** One writer per file, and its lock isn't fair: a
  process that finds it taken backs off with growing sleeps, while the holder takes it again
  at once, so one or two workers end up doing almost everything. Fairer waiting wouldn't add
  throughput — the lock serializes them either way — so run one worker process.

## 3. End to end, and harel next to DBOS

Here the enqueue is timed too: the window runs from the first `start()`/`send()` to the last
event processed. That is how [DBOS](https://www.dbos.dev/) is measured, so it is the comparison
below. The DBOS side is `bench_dbos.py` (dbos 3.2), on the same Postgres; variant A is one
durable workflow per execution that `recv`s the two events, variant B one durable workflow per
event running a transaction.

This is a **paradigm comparison, not a benchmark of DBOS**: harel is a statechart engine, DBOS
runs arbitrary imperative code durably and does a workflow's bookkeeping per event — the right
tool for that job, and more than this toy needs. DBOS ran in its default configuration.

### Clients and workers in separate processes

`bench_workers.py --producers P` and `bench_dbos.py --workers W --producers P`: W worker
processes, P client processes that send the events, 1000 executions (2000 events), Postgres.
Two runs each, events/s.

| | W=1 | W=2 | W=4 |
|---|---:|---:|---:|
| harel, P=1 | 527 / 491 | 564 / 529 | 539 / 570 |
| harel, P=4 | 847 / 882 | 932 / 880 | 813 / 848 |
| harel, P=8 | 654 / 655 | 807 / 754 | 876 / 695 |
| DBOS A, P=1 | 259 / 229 | 372 / 321 | 457 / 488 |
| DBOS A, P=4 | 203 / 208 | 357 / 346 | 496 / 437 |
| DBOS A, P=8 | 210 / 193 | 316 / 312 | 435 / 423 |

- **harel with one client** (~530) is bound by that client: each `start()` is about four
  sequential round-trips (load, commit with the `Start` in the outbox, publish, ack), and more
  workers don't change the rate.
- **harel with four clients** reaches ~850–930 with a single worker, close to the ~1100 that
  Postgres gives draining a backlog (section 2). More workers don't raise it.
- **Eight clients** don't help: 8 clients + 4 workers + Postgres on 8 cores.
- **DBOS scales with workers** (≈230, ≈350, ≈470 for 1, 2, 4) and not with clients: its limit is
  running the workflows. It was still rising at 4 workers; this host can't run more processes
  without them competing for cores.
- Same configuration, harel is ahead throughout: about 4× with one worker (≈860 against ≈205 at
  P=4), about 1.8× with four (≈830 against ≈465). Best case of each: ≈900 against ≈470–500.

### Everything in one process

`bench_async.py --no-sleep --e2e --n-executions 1000` and `bench_dbos.py --n 1000`: one process
both sends and processes. Median of 3 runs, events/s.

| | events/s |
|---|---:|
| harel (c=1 / 16 / 64) | 574 / 588 / 594 |
| DBOS A — workflow + `send`/`recv` | 261 |
| DBOS B — workflow per event | 163 |

A single Python process is CPU-bound here — harel's spends ~85% of the run's wall time on CPU —
because the same core does the client's work and the worker's. Concurrency doesn't change it, and neither do more enqueueing coroutines or threads
(`--producers`: harel 656 / 742 / 630 with 1 / 8 / 32, DBOS A 258 / 217 / 219). Read this table
as work per event on one core: harel does about 2.3× DBOS A's events per second there.

## Not measured

- Sharding across several Redis instances (`bench_shards.py`).
- The default 10 ms action (without `--no-sleep`).
- DynamoDB/SQS: LocalStack simulates AWS, and its latency says nothing about the real service.
- libSQL.

## Reproducing

From the repo root, with the stack's backends up and no worker running:

```text
docker compose -f deploy/docker-compose.yml up -d --wait redis postgres rqlite mongo
docker compose -f deploy/docker-compose.yml run --rm --no-deps -v "$PWD/bench:/app/bench:ro" \
    -e HAREL_STORE_BACKEND=postgres -e HAREL_TRANSPORT_BACKEND=postgres \
    test /app/.venv/bin/python bench/bench_workers.py --n-executions 2000 --workers 1,2,4,8
```

For SQLite set `HAREL_STORE_DB` and `HAREL_TRANSPORT_DB` to two paths under `/state`. For DBOS,
`uv pip install --python /app/.venv/bin/python dbos` inside the container and set
`HAREL_DBOS_DSN=postgresql://stm:stm@postgres:5432/dbosbench`.
