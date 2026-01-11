"""A long-lived worker process: drives Executions off a Transport, forever.

Configured entirely by environment variables so it runs as a docker-compose
service scaled to N replicas (each replica is one `Worker` loop):

    STM_REDIS_URL        redis URL for the transport (the queue)
    STM_STORE_BACKEND    "sqlite" (default), "redis", or "postgres" for the store
    STM_STORE_DB         sqlite file for the store        (sqlite backend; a shared volume)
    STM_STORE_REDIS_URL  redis URL for the store, defaults to STM_REDIS_URL (redis backend)
    STM_POSTGRES_DSN     postgres DSN for the store       (postgres backend)
    STM_RQLITE_URL       rqlite HTTP base URL for the store (rqlite backend)
    STM_SQS_ENDPOINT     SQS endpoint URL (e.g. LocalStack) for the sqs transport
    STM_SQS_QUEUE        SQS FIFO queue name (default stm.fifo)
    STM_DEFINITIONS_DIR  directory of *.stm machine files = the Definition registry
    STM_WORKER_ID        worker id (defaults to the hostname)
    STM_VISIBILITY       lease seconds while a message is in flight (default 30)

Pure-sqlite (single machine / shared volume), pure-redis (all-network) and
postgres (distributed SQL) are all supported by swapping STM_STORE_BACKEND.

Run with: `python -m harel.worker`. SIGTERM/SIGINT stop the loop cleanly. The
workers share nothing but the store + the transport — separate processes (here,
containers) coordinating by events, which is the whole point of the model.
"""

from __future__ import annotations

import os
import signal
import socket
import threading
from pathlib import Path

from harel.definition.model import Definition
from harel.definition.validate import ValidationError
from harel.dsl import definition_from_dsl_file, parse
from harel.dsl.parser import DslError
from harel.engine.distributed import Worker
from harel.engine.store import ExecutionStore, PostgresStore, RedisStore, RqliteStore, SqliteStore
from harel.engine.transport import (
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
    """Build the durable store from STM_STORE_BACKEND (sqlite | redis)."""
    backend = os.environ.get("STM_STORE_BACKEND", "sqlite")
    if backend == "sqlite":
        return SqliteStore(os.environ["STM_STORE_DB"])
    if backend == "redis":
        return RedisStore.from_url(os.environ.get("STM_STORE_REDIS_URL") or os.environ["STM_REDIS_URL"])
    if backend == "postgres":
        return PostgresStore.from_dsn(os.environ["STM_POSTGRES_DSN"])
    if backend == "rqlite":
        return RqliteStore.from_url(os.environ["STM_RQLITE_URL"])
    raise ValueError(f"unknown STM_STORE_BACKEND: {backend}")


def build_transport() -> Transport:
    """Build the event transport from STM_TRANSPORT_BACKEND (redis | postgres |
    rqlite | sqlite). Default redis; postgres/rqlite give a no-Redis stack (one
    SQL backend can serve both store and transport)."""
    backend = os.environ.get("STM_TRANSPORT_BACKEND", "redis")
    if backend == "redis":
        return RedisTransport.from_url(os.environ["STM_REDIS_URL"])
    if backend == "postgres":
        return PostgresTransport.from_dsn(os.environ["STM_POSTGRES_DSN"])
    if backend == "rqlite":
        return RqliteTransport.from_url(os.environ["STM_RQLITE_URL"])
    if backend == "sqlite":
        return SqliteTransport(os.environ["STM_TRANSPORT_DB"])
    if backend == "sqs":
        return SqsTransport.create(
            os.environ["STM_SQS_ENDPOINT"], os.environ.get("STM_SQS_QUEUE", "stm.fifo")
        )
    raise ValueError(f"unknown STM_TRANSPORT_BACKEND: {backend}")


def main() -> None:
    definitions_dir = os.environ["STM_DEFINITIONS_DIR"]
    worker_id = os.environ.get("STM_WORKER_ID", socket.gethostname())
    visibility = float(os.environ.get("STM_VISIBILITY", "30"))

    store = build_store()
    transport = build_transport()
    definitions = load_definitions(definitions_dir)

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    try:
        Worker(store, transport, definitions, worker_id=worker_id, visibility=visibility).run(stop)
    finally:
        store.close()
        transport.close()


if __name__ == "__main__":
    main()
