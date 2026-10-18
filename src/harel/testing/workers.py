"""The contract of the workers' heartbeats: `assert_workers_contract(store)` (and
`assert_async_workers_contract`). harel runs it on its own backends; run it on yours.

The contract: `list_workers` returns each worker's last `record_worker`, by `worker_id` — a
newer one replaces the one before, machines and all — and `forget_worker` removes one (a no-op
for one never recorded). Everything is namespaced by `ns`, so it holds on a shared backend.
"""

from harel.engine.store import WorkerRecord


def _records(ns: str) -> tuple[WorkerRecord, WorkerRecord, WorkerRecord]:
    first = WorkerRecord(f"{ns}w1", None, 100.0, 100.0, 10.0)
    dedicated = WorkerRecord(f"{ns}w2", ("order", "payment"), 101.0, 101.0, 10.0)
    later = WorkerRecord(f"{ns}w1", ("order",), 100.0, 130.5, 10.0)
    return first, dedicated, later


def _mine(workers, ns: str) -> list:
    return [w for w in workers if w.worker_id.startswith(ns)]


def assert_workers_contract(store, ns: str = "wk-") -> None:
    first, dedicated, later = _records(ns)
    assert _mine(store.list_workers(), ns) == []
    store.record_worker(first)
    store.record_worker(dedicated)
    assert _mine(store.list_workers(), ns) == [first, dedicated], "every worker's heartbeat, by worker_id"
    store.record_worker(later)
    assert _mine(store.list_workers(), ns) == [later, dedicated], "a newer heartbeat replaces the one before"
    store.forget_worker(dedicated.worker_id)
    store.forget_worker(f"{ns}never")
    assert _mine(store.list_workers(), ns) == [later], "a forgotten worker is gone"
    store.forget_worker(later.worker_id)


async def assert_async_workers_contract(store, ns: str = "wk-") -> None:
    first, dedicated, later = _records(ns)
    assert _mine(await store.list_workers(), ns) == []
    await store.record_worker(first)
    await store.record_worker(dedicated)
    assert _mine(await store.list_workers(), ns) == [first, dedicated]
    await store.record_worker(later)
    assert _mine(await store.list_workers(), ns) == [later, dedicated]
    await store.forget_worker(dedicated.worker_id)
    await store.forget_worker(f"{ns}never")
    assert _mine(await store.list_workers(), ns) == [later]
    await store.forget_worker(later.worker_id)
