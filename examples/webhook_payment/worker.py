"""Event + timer worker — consumes from the transport and fires due timers.

Run this as a separate process alongside the API server:

    uvicorn examples.webhook_payment.app:app        # terminal 1
    python -m examples.webhook_payment.worker       # terminal 2

Worker.run() loops: claim → process → ack, then fire_due_timers() on idle.
The API server and this worker share the same two SQLite files (the store and the
queue; WAL mode); they never share process memory. Run one worker process: SQLite has
one writer per file, so more don't add throughput.

Separation of concerns:
  API server  — request/response only: create executions, publish webhook events
                to the transport.  Returns 204 before anything is processed.
  This worker — consumes events from the transport, runs the engine, fires timers.
                A slow action never delays a webhook response.
"""

import logging
import threading
from pathlib import Path

from harel import SqliteStore, definition_from_dsl_file
from harel.engine.distributed import DistributedRunner
from harel.engine.transport.sqlite import SqliteTransport

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

PAYMENT_STM = Path(__file__).parent / "payment.stm"
STORE_DB = Path(__file__).parent / "payments.db"  # the same two files as app.py
QUEUE_DB = Path(__file__).parent / "payments-queue.db"

defn = definition_from_dsl_file(PAYMENT_STM, "payment")
store = SqliteStore(STORE_DB)
transport = SqliteTransport(QUEUE_DB)
runner = DistributedRunner(store, transport, {defn.id: defn})

if __name__ == "__main__":
    logging.info("worker started")
    stop = threading.Event()
    try:
        runner.worker(worker_id="payment-worker").run(stop)
    except KeyboardInterrupt:
        stop.set()
