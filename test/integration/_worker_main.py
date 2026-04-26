"""Standalone worker entry point for the multi-process integration tests.

Run as a script in its own process:

    python _worker_main.py <store_db> <transport_kind> <transport_dsn> <def_file> <machine> <worker_id>

`transport_kind` is "sqlite" (dsn = the queue db file) or "redis" (dsn = a redis
URL). It rebuilds the Definition from the DSL file (its id is the machine name,
stable across processes, so it matches the `definition_id` stored by the creator),
opens its own connections, and runs a Worker loop until SIGTERM. Nothing is shared
with the parent but the durable store + the transport — the whole point of the
distributed model.
"""

import signal
import sys
import threading
from pathlib import Path

from harel.dsl import definition_from_dsl
from harel.engine.distributed import Worker
from harel.engine.store import SqliteStore
from harel.engine.transport import RedisTransport, SqliteTransport


def _build_transport(kind: str, dsn: str):
    if kind == "sqlite":
        return SqliteTransport(dsn)
    if kind == "redis":
        return RedisTransport.from_url(dsn)
    raise ValueError(f"unknown transport kind: {kind}")


def main(argv: list[str]) -> None:
    store_db, transport_kind, transport_dsn, def_file, machine, worker_id = argv
    # The DSL is the front-end; the machine id is the machine name, stable across
    # processes, so it matches the definition_id stored by the creator.
    defn = definition_from_dsl(Path(def_file).read_text(), machine)
    store = SqliteStore(store_db)
    transport = _build_transport(transport_kind, transport_dsn)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        Worker(store, transport, {defn.id: defn}, worker_id=worker_id, visibility=30.0).run(
            stop, idle_sleep=0.005
        )
    finally:
        store.close()
        if hasattr(transport, "close"):
            transport.close()


if __name__ == "__main__":
    main(sys.argv[1:])
