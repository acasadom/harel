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
make test-stack   # shortcut: brings up the default stack (sqlite store + redis transport), runs tests, tears down
```

Or manually:

```bash
docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis worker
docker compose -f deploy/docker-compose.yml run --rm test
docker compose -f deploy/docker-compose.yml down -v   # -v drops the state volume
```

Scale workers with `--scale worker=N`. Tail logs with
`docker compose -f deploy/docker-compose.yml logs -f worker`.

### Store backend: sqlite, redis, postgres, rqlite, mongo, libsql, or dynamodb

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

# mongo (document store): everything for one Execution in a single document,
# so a commit is one atomic update_one (no replica set needed).
STM_STORE_BACKEND=mongo docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis mongo worker
STM_STORE_BACKEND=mongo docker compose -f deploy/docker-compose.yml run --rm test

# dynamodb (AWS serverless, on localstack): conditional writes are the CAS and
# TransactWriteItems makes the commit atomic. Pairs with the sqs transport for an
# all-AWS stack. No AWS account needed.
STM_STORE_BACKEND=dynamodb docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis localstack worker
STM_STORE_BACKEND=dynamodb docker compose -f deploy/docker-compose.yml run --rm test

# libsql (Turso's SQLite fork) — EXPERIMENTAL (local-file path tested in-process; the
# Turso/sqld path below needs a Turso account / sqld server to validate). STM_LIBSQL_DB is a
# local file on the shared /state volume (single machine, like sqlite). For distributed, set
# STM_LIBSQL_SYNC_URL (+ STM_LIBSQL_AUTH_TOKEN) to a Turso/sqld primary — each worker keeps a
# synced embedded replica.
STM_STORE_BACKEND=libsql docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis worker
STM_STORE_BACKEND=libsql docker compose -f deploy/docker-compose.yml run --rm test
```

sqlite / libsql-file = single machine (or one host's containers); redis / postgres / rqlite /
mongo / dynamodb / libsql-on-Turso = distributed across machines. Same engine, workers
and test for all of them. The
test service runs `pytest -m stack`, so the contract tests for the active backend
run against the real service.

### Transport backend (the queue)

`STM_TRANSPORT_BACKEND` selects the event transport independently of the store
(`redis` default, or `postgres` / `rqlite` / `sqlite` / `mongo` / `libsql` / `sqs`). So you
can run **no Redis at all** — one backend for both state and queue:

```bash
# all-postgres: state + queue both on Postgres, no Redis
STM_STORE_BACKEND=postgres STM_TRANSPORT_BACKEND=postgres \
  docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 postgres worker
STM_STORE_BACKEND=postgres STM_TRANSPORT_BACKEND=postgres \
  docker compose -f deploy/docker-compose.yml run --rm test

# all-rqlite likewise (STM_STORE_BACKEND=rqlite STM_TRANSPORT_BACKEND=rqlite, bring up `rqlite`)

# all-libsql likewise (STM_STORE_BACKEND=libsql STM_TRANSPORT_BACKEND=libsql) — file mode on the
# shared volume, or point STM_LIBSQL_SYNC_URL at Turso/sqld for distributed

# all-mongo: state + queue both on MongoDB, no Redis. Bring up the `mongo` service.
STM_STORE_BACKEND=mongo STM_TRANSPORT_BACKEND=mongo \
  docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 mongo worker
STM_STORE_BACKEND=mongo STM_TRANSPORT_BACKEND=mongo \
  docker compose -f deploy/docker-compose.yml run --rm test

# SQS FIFO via LocalStack (no AWS account): the queue's MessageGroupId is the
# per-group exclusivity natively. Bring up the `localstack` service.
STM_TRANSPORT_BACKEND=sqs \
  docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 localstack worker
STM_TRANSPORT_BACKEND=sqs docker compose -f deploy/docker-compose.yml run --rm test

# all-AWS serverless: DynamoDB store + SQS transport, both on localstack, no Redis.
STM_STORE_BACKEND=dynamodb STM_TRANSPORT_BACKEND=sqs \
  docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 localstack worker
STM_STORE_BACKEND=dynamodb STM_TRANSPORT_BACKEND=sqs \
  docker compose -f deploy/docker-compose.yml run --rm test
```

Store and transport are independent: mix them (e.g. `STM_STORE_BACKEND=postgres`
with the default Redis transport) or unify on one backend.

### FaaS remote actions (Lambda on LocalStack)

The `faas` extra runs an action's impl as a remote function (see
`docs/guide/faas.md`). `test/integration/test_faas_lambda.py` invokes a **real**
Lambda on LocalStack to cover the boto3 transport end to end. The action
(`deploy/faas_function.py` = `handler(charge)`) is zip-packaged on the Lambda base
image so pydantic-core's binary matches the runtime (LocalStack community doesn't
support image packaging — Pro only):

```bash
# 1. build the deployment zip (linux/amd64 so it matches the x86_64 Lambda)
docker build --platform=linux/amd64 -f deploy/Dockerfile.lambda \
  --target export --output deploy/build .

# 2. bring up localstack (it launches the Lambda container via the mounted docker
#    socket) and run the stack tests — the test uploads deploy/build/function.zip
docker compose -f deploy/docker-compose.yml up -d localstack
docker compose -f deploy/docker-compose.yml run --rm test
```

Skips automatically if the zip isn't built / `STM_LAMBDA_*` aren't set. The HTTP
transport (OpenFaaS / Spin / Cloudflare / Knative) is covered deterministically by a
real in-thread HTTP server in `test/unit/faas/` — no Docker needed.
