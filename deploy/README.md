# Distributed stack (docker-compose)

A realistic deployment of the engine: **Redis** as the event transport and **N
worker replicas** (separate containers) that wait for events, sharing the durable
store on a Docker **named volume**. This is the production shape — workers are
long-lived processes you scale, not something a test spawns.

## Pieces

- `Dockerfile` — one image for both services: the project + the `redis` extra +
  dev deps. Workers run `python -m harel.worker`; the test service runs pytest.
- `docker-compose.yml` — `redis` + a `worker` service scaled to N replicas + a
  `test` service, all sharing a named volume `state` mounted at `/state` (the
  shared sqlite store).
- `definitions/` — the machine `.stm` files the workers load (`flat`, `ortho`). A
  Definition's id is its machine name, stable across processes.
- `demo_actions.py` — the actions those machines reference (`demo_actions.rec`).

## Why the test runs in a container

The store is a single sqlite file shared between the workers and the test. SQLite
**WAL** needs a shared-memory `-shm` file (mmap) that all accessors coordinate
through — which works only when they share a kernel + a real filesystem. A macOS
Docker Desktop **bind mount** goes through virtiofs, where that `-shm` mmap does
**not** work, so the host (macOS) and the workers (the Linux VM) can't share a WAL
sqlite reliably. The fix: keep the store on a **named volume** (in the VM, ext4)
and run the **test as a container too** — then test + workers share the VM kernel
and WAL works, exactly like the host-only multi-process test
(`test_multiprocess_workers.py`). On a native Linux host a bind mount would be
fine; the named volume is portable either way.

## Run the stack + its integration test

```bash
docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis worker
docker compose -f deploy/docker-compose.yml run --rm test
docker compose -f deploy/docker-compose.yml down -v   # -v drops the state volume
```

Scale workers with `--scale worker=N`. Tail logs with
`docker compose -f deploy/docker-compose.yml logs -f worker`.

### Store backend: sqlite, redis, postgres, or rqlite

`STM_STORE_BACKEND` selects the durable store (default `sqlite`, the shared
volume above). The others are **all-network** (no shared filesystem); bring up
the matching service:

```bash
# pure-redis: state + queue both in Redis, no volume needed
STM_STORE_BACKEND=redis docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis worker
STM_STORE_BACKEND=redis docker compose -f deploy/docker-compose.yml run --rm test

# postgres (distributed SQL):
STM_STORE_BACKEND=postgres docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis postgres worker
STM_STORE_BACKEND=postgres docker compose -f deploy/docker-compose.yml run --rm test

# rqlite (distributed SQLite, Raft):
STM_STORE_BACKEND=rqlite docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis rqlite worker
STM_STORE_BACKEND=rqlite docker compose -f deploy/docker-compose.yml run --rm test
```

sqlite = single machine (or one host's containers); redis / postgres / rqlite =
distributed across machines. Same engine, workers and test for all of them. The
test service runs `pytest -m stack`, so the contract tests for the active backend
run against the real service.

### Transport backend (the queue)

`STM_TRANSPORT_BACKEND` selects the event transport independently of the store
(`redis` default, or `postgres` / `rqlite` / `sqlite`). So you can run **no Redis
at all** — one SQL backend for both state and queue:

```bash
# all-postgres: state + queue both on Postgres, no Redis
STM_STORE_BACKEND=postgres STM_TRANSPORT_BACKEND=postgres \
  docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 postgres worker
STM_STORE_BACKEND=postgres STM_TRANSPORT_BACKEND=postgres \
  docker compose -f deploy/docker-compose.yml run --rm test

# all-rqlite likewise (STM_STORE_BACKEND=rqlite STM_TRANSPORT_BACKEND=rqlite, bring up `rqlite`)

# SQS FIFO via LocalStack (no AWS account): the queue's MessageGroupId is the
# per-group exclusivity natively. Bring up the `localstack` service.
STM_TRANSPORT_BACKEND=sqs \
  docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 localstack worker
STM_TRANSPORT_BACKEND=sqs docker compose -f deploy/docker-compose.yml run --rm test
```

Store and transport are independent: mix them (e.g. `STM_STORE_BACKEND=postgres`
with the default Redis transport) or unify on one backend.
