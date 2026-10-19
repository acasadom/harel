"""Opening a SQLite file several processes share (the store and the transport, sync and async)."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from typing import Any

# switching to WAL takes an exclusive lock, which SQLite doesn't wait for: workers opening one
# file at once retry it — for up to `_WAL_TRIES * _WAL_PAUSE` seconds
_WAL_TRIES = 50
_WAL_PAUSE = 0.1


def _locked(exc: sqlite3.OperationalError) -> bool:
    return "locked" in str(exc) or "busy" in str(exc)


def enable_wal(conn: Any) -> None:
    """Wait for the write lock instead of erroring (`busy_timeout`), then put the file in WAL
    mode — readers don't block the single writer — retrying while another opener holds it."""
    conn.execute("PRAGMA busy_timeout=5000")
    for attempt in range(_WAL_TRIES):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as exc:
            if not _locked(exc) or attempt == _WAL_TRIES - 1:
                raise
            time.sleep(_WAL_PAUSE)


async def aenable_wal(conn: Any) -> None:
    """`enable_wal` over an aiosqlite connection."""
    await conn.execute("PRAGMA busy_timeout=5000")
    for attempt in range(_WAL_TRIES):
        try:
            await conn.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as exc:
            if not _locked(exc) or attempt == _WAL_TRIES - 1:
                raise
            await asyncio.sleep(_WAL_PAUSE)
