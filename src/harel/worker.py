"""A long-lived worker process: drives Executions off a Transport, forever.

Configured entirely by environment variables so it runs as a docker-compose
service scaled to N replicas (each replica is one `Worker` loop):

    STM_REDIS_URL        redis URL for the transport (the queue)
    STM_STORE_BACKEND    "sqlite" (default), "redis", "postgres", "rqlite", "mongo", "libsql", "dynamodb"
    STM_STORE_DB         sqlite file for the store        (sqlite backend; a shared volume)
    STM_LIBSQL_DB        libSQL database file (libsql backend; store + transport share it)
    STM_LIBSQL_SYNC_URL  Turso/sqld URL for an embedded replica (optional); STM_LIBSQL_AUTH_TOKEN its token
    STM_STORE_REDIS_URL  redis URL for the store, defaults to STM_REDIS_URL (redis backend)
    STM_POSTGRES_DSN     postgres DSN for the store       (postgres backend)
    STM_RQLITE_URL       rqlite HTTP base URL for the store (rqlite backend)
    STM_MONGO_URL        mongodb URL for the store/transport (mongo backend)
    STM_MONGO_DB         mongodb database name (mongo backend; default "harel")
    STM_SQS_ENDPOINT     SQS endpoint URL (e.g. LocalStack) for the sqs transport
    STM_SQS_QUEUE        SQS FIFO queue name (default stm.fifo)
    STM_DYNAMODB_ENDPOINT DynamoDB endpoint URL (e.g. LocalStack); unset = real AWS (dynamodb store)
    STM_AWS_REGION       AWS region for the dynamodb store (default us-east-1)
    STM_DEFINITIONS_DIR  directory of *.stm machine files = the Definition registry
    STM_WORKER_ID        worker id (defaults to the hostname)
    STM_VISIBILITY       lease seconds while a message is in flight (default 30)
    STM_CONCURRENCY      max events in flight on the loop at once (default 256)

Pure-sqlite (single machine / shared volume), pure-redis (all-network), postgres
(distributed SQL) and mongo (document store) are all supported by swapping
STM_STORE_BACKEND.

The worker is **async-native**: one `asyncio` event loop (via `anyio.run`) drives up to
`STM_CONCURRENCY` events in flight at once with `AsyncWorker.run` (the throughput win).
All backends have a native async port and run async end-to-end.

Run with: `python -m harel.worker`. SIGTERM/SIGINT stop the loop cleanly. The
workers share nothing but the store + the transport — separate processes (here,
containers) coordinating by events, which is the whole point of the model.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from pathlib import Path
from typing import Any

from harel.definition.model import Definition
from harel.definition.validate import ValidationError
from harel.dsl import definition_from_dsl_file, parse
from harel.dsl.parser import DslError
from harel.engine.aio.distributed import AsyncWorker
from harel.engine.store import (
    DynamoDBStore,
    ExecutionStore,
    LibsqlStore,
    MongoStore,
    PostgresStore,
    RedisStore,
    RqliteStore,
    SqliteStore,
)
from harel.engine.transport import (
    LibsqlTransport,
    MongoTransport,
    PostgresTransport,
    RedisTransport,
    RqliteTransport,
    SqliteTransport,
    SqsTransport,
    Transport,
)


def load_definitions(definitions_dir: str) -> dict[str, Definition]:
    """Build a registry from every machine in every ``*.stm`` under the dir,
    **validating each** so the worker fails fast (with the offending file/machine
    named) instead of loading a structurally broken machine that only misbehaves at
    run time. A Definition's id is its machine name, so it matches what creators store."""
    registry: dict[str, Definition] = {}
    for path in sorted(Path(definitions_dir).glob("*.stm")):
        for name in parse(path.read_text()).machines:
            try:
                defn = definition_from_dsl_file(path, name, validate=True)
            except (DslError, ValidationError) as e:
                raise RuntimeError(f"invalid machine {name!r} in {path}: {e}") from e
            registry[defn.id] = defn
    return registry


def build_store() -> ExecutionStore:
    """Build the durable store from STM_STORE_BACKEND (sqlite | redis | postgres |
    rqlite | mongo | libsql | dynamodb)."""
    backend = os.environ.get("STM_STORE_BACKEND", "sqlite")
    if backend == "sqlite":
        return SqliteStore(os.environ["STM_STORE_DB"])
    if backend == "redis":
        return RedisStore.from_url(os.environ.get("STM_STORE_REDIS_URL") or os.environ["STM_REDIS_URL"])
    if backend == "postgres":
        return PostgresStore.from_dsn(os.environ["STM_POSTGRES_DSN"])
    if backend == "rqlite":
        return RqliteStore.from_url(os.environ["STM_RQLITE_URL"])
    if backend == "mongo":
        return MongoStore.from_url(os.environ["STM_MONGO_URL"], os.environ.get("STM_MONGO_DB", "harel"))
    if backend == "libsql":
        return LibsqlStore(os.environ["STM_LIBSQL_DB"], **_libsql_kwargs())
    if backend == "dynamodb":
        return DynamoDBStore.create(
            os.environ.get("STM_DYNAMODB_ENDPOINT"), os.environ.get("STM_AWS_REGION", "us-east-1")
        )
    raise ValueError(f"unknown STM_STORE_BACKEND: {backend}")


def _libsql_kwargs() -> dict[str, Any]:
    """libSQL connection args from the env. STM_LIBSQL_DB is the local database file (used by
    store + transport). For an embedded replica against a Turso/`sqld` primary, set
    STM_LIBSQL_SYNC_URL (+ STM_LIBSQL_AUTH_TOKEN); otherwise it is a plain local libSQL file."""
    kwargs: dict[str, Any] = {}
    sync_url = os.environ.get("STM_LIBSQL_SYNC_URL")
    if sync_url:
        kwargs["sync_url"] = sync_url
        kwargs["auth_token"] = os.environ.get("STM_LIBSQL_AUTH_TOKEN", "")
    return kwargs


def build_transport() -> Transport:
    """Build the event transport from STM_TRANSPORT_BACKEND (redis | postgres |
    rqlite | sqlite | mongo | libsql | sqs). Default redis; postgres/rqlite/mongo/libsql
    give a no-Redis stack (one backend serves store + transport)."""
    backend = os.environ.get("STM_TRANSPORT_BACKEND", "redis")
    if backend == "redis":
        return RedisTransport.from_url(os.environ["STM_REDIS_URL"])
    if backend == "postgres":
        return PostgresTransport.from_dsn(os.environ["STM_POSTGRES_DSN"])
    if backend == "rqlite":
        return RqliteTransport.from_url(os.environ["STM_RQLITE_URL"])
    if backend == "sqlite":
        return SqliteTransport(os.environ["STM_TRANSPORT_DB"])
    if backend == "mongo":
        return MongoTransport.from_url(os.environ["STM_MONGO_URL"], os.environ.get("STM_MONGO_DB", "harel"))
    if backend == "libsql":
        return LibsqlTransport(os.environ["STM_LIBSQL_DB"], **_libsql_kwargs())
    if backend == "sqs":
        return SqsTransport.create(
            os.environ["STM_SQS_ENDPOINT"], os.environ.get("STM_SQS_QUEUE", "stm.fifo")
        )
    raise ValueError(f"unknown STM_TRANSPORT_BACKEND: {backend}")


async def build_store_async() -> Any:
    """The async store for STM_STORE_BACKEND: all backends have a native async port."""
    backend = os.environ.get("STM_STORE_BACKEND", "sqlite")
    if backend == "sqlite":
        from harel.engine.aio_store import AsyncSqliteStore

        return await AsyncSqliteStore.create(os.environ["STM_STORE_DB"])
    if backend == "redis":
        from harel.engine.aio_store import AsyncRedisStore

        return AsyncRedisStore.from_url(os.environ.get("STM_STORE_REDIS_URL") or os.environ["STM_REDIS_URL"])
    if backend == "postgres":
        from harel.engine.aio_store import AsyncPostgresStore

        return await AsyncPostgresStore.from_dsn(os.environ["STM_POSTGRES_DSN"])
    if backend == "mongo":
        from harel.engine.aio_store import AsyncMongoStore

        return await AsyncMongoStore.from_url(
            os.environ["STM_MONGO_URL"], os.environ.get("STM_MONGO_DB", "harel")
        )
    if backend == "rqlite":
        from harel.engine.aio_store import AsyncRqliteStore

        return await AsyncRqliteStore.from_url(os.environ["STM_RQLITE_URL"])
    if backend == "libsql":
        from harel.engine.aio_store import AsyncLibsqlStore

        return await AsyncLibsqlStore.create(os.environ["STM_LIBSQL_DB"], **_libsql_kwargs())
    if backend == "dynamodb":
        from harel.engine.aio_store import AsyncDynamoDBStore

        return await AsyncDynamoDBStore.create(
            endpoint_url=os.environ.get("STM_DYNAMODB_ENDPOINT"),
            region=os.environ.get("STM_AWS_REGION", "us-east-1"),
        )
    raise ValueError(f"unknown STM_STORE_BACKEND: {backend}")


async def build_transport_async() -> Any:
    """The async transport for STM_TRANSPORT_BACKEND: all backends have a native async port."""
    backend = os.environ.get("STM_TRANSPORT_BACKEND", "redis")
    if backend == "redis":
        from harel.engine.aio_transport import AsyncRedisTransport

        return AsyncRedisTransport.from_url(os.environ["STM_REDIS_URL"])
    if backend == "sqlite":
        from harel.engine.aio_transport import AsyncSqliteTransport

        return await AsyncSqliteTransport.create(os.environ["STM_TRANSPORT_DB"])
    if backend == "postgres":
        from harel.engine.aio_transport import AsyncPostgresTransport

        return await AsyncPostgresTransport.from_dsn(os.environ["STM_POSTGRES_DSN"])
    if backend == "mongo":
        from harel.engine.aio_transport import AsyncMongoTransport

        return await AsyncMongoTransport.from_url(
            os.environ["STM_MONGO_URL"], os.environ.get("STM_MONGO_DB", "harel")
        )
    if backend == "rqlite":
        from harel.engine.aio_transport import AsyncRqliteTransport

        return await AsyncRqliteTransport.from_url(os.environ["STM_RQLITE_URL"])
    if backend == "libsql":
        from harel.engine.aio_transport import AsyncLibsqlTransport

        return await AsyncLibsqlTransport.create(os.environ["STM_LIBSQL_DB"], **_libsql_kwargs())
    if backend == "sqs":
        from harel.engine.aio_transport import AsyncSqsTransport

        return await AsyncSqsTransport.create(
            endpoint_url=os.environ.get("STM_SQS_ENDPOINT"),
            queue_name=os.environ.get("STM_SQS_QUEUE", "stm.fifo"),
            region=os.environ.get("STM_AWS_REGION", "us-east-1"),
        )
    raise ValueError(f"unknown STM_TRANSPORT_BACKEND: {backend}")


async def amain() -> None:
    definitions_dir = os.environ["STM_DEFINITIONS_DIR"]
    worker_id = os.environ.get("STM_WORKER_ID", socket.gethostname())
    visibility = float(os.environ.get("STM_VISIBILITY", "30"))
    concurrency = int(os.environ.get("STM_CONCURRENCY", "256"))

    store = await build_store_async()
    transport = await build_transport_async()
    definitions = load_definitions(definitions_dir)  # sync startup IO, one-time

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    worker = AsyncWorker(
        store, transport, definitions, worker_id=worker_id, visibility=visibility, concurrency=concurrency
    )
    try:
        await worker.run(stop)
    finally:
        await store.close()
        await transport.close()


def main() -> None:
    import anyio

    anyio.run(amain)


if __name__ == "__main__":
    main()
