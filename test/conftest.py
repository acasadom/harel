"""Suite-wide fixtures."""

import pytest


@pytest.fixture(autouse=True)
def _stop_unclosed_aiosqlite_connections(monkeypatch):
    """Stop, after each test, every aiosqlite connection it opened and didn't close.

    Each aiosqlite connection runs on its own non-daemon thread, which only ends on
    `close()`. A test that fails before closing its async SQLite store or transport keeps
    the connection alive (pytest holds the failure's traceback, and with it the store), so
    the thread outlives the test and the pytest process never exits."""
    try:
        from aiosqlite import Connection
    except ImportError:
        yield
        return
    opened = []
    init = Connection.__init__

    def tracking_init(self, *args, **kwargs):
        init(self, *args, **kwargs)
        opened.append(self)

    monkeypatch.setattr(Connection, "__init__", tracking_init)
    yield
    for conn in opened:
        if conn._thread.is_alive():
            conn.stop()  # closes the sqlite connection on its thread, then ends the thread
            conn._thread.join(timeout=5)


def pytest_addoption(parser):
    parser.addoption(
        "--execution",
        default="background",
        choices=("background", "inline"),
        help="the execution model the sync runners default to (run the suite under each)",
    )


@pytest.fixture(autouse=True)
def _sync_runners_execution(request, monkeypatch):
    """With `--execution=inline`, every sync runner, worker and bare `Driver` a test builds
    runs in the caller's thread unless the test picks a model itself — the same tests, another model."""
    execution = request.config.getoption("--execution")
    if execution == "background":
        yield
        return
    import functools

    from harel.engine import distributed, durable, runtime

    for cls in (durable.DurableRunner, distributed.DistributedRunner, distributed.Worker, runtime.Driver):
        init = cls.__init__

        @functools.wraps(init)
        def defaulted(self, *args, __init=init, **kwargs):
            kwargs.setdefault("execution", execution)
            __init(self, *args, **kwargs)

        monkeypatch.setattr(cls, "__init__", defaulted)
    yield
