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
