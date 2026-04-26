# Integration tests

Tests for the distributed STM system (durable runners, Executions coordinating
by events over a `Transport`). Unit tests (single process, single thread) live
under `test/unit/`.

- `test_distributed_workers.py` — **multi-thread**: N `Worker` threads race for
  work over one shared `SqliteStore` + `SqliteTransport`. Proves per-group
  exclusivity (each Execution single-writer), concurrency across Executions, and
  orthogonal fan-out + join across workers.
- `test_multiprocess_workers.py` — **multi-process**: workers are real
  subprocesses (`_worker_main.py`) that share *nothing* with the parent but the
  two sqlite files; each rebuilds the Definition from YAML (its id = the machine
  name, stable across processes) and drives Executions off the transport. WAL
  lets the processes share the files. Flat machines advance and orthogonal
  machines fan out + join entirely across process boundaries.
