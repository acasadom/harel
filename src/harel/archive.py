"""Archivers for `purge`: where a finished execution tree goes before it is deleted.

An archiver is any callable taking the bundle `purge` builds (`root_id`, the tree's
`executions` root first, and each one's `traces`); an async runner also accepts a
coroutine function. It runs before anything is deleted, so raising aborts the purge.
A retried purge may hand it the same `root_id` again — a consumer keys on it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Union


class JsonlArchive:
    """Append each purged tree as one JSON line to `path` (created if absent). The line is
    flushed and fsynced before returning, so it is durable before `purge` deletes anything."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)

    def __call__(self, bundle: dict) -> None:
        line = json.dumps(bundle, separators=(",", ":"), ensure_ascii=False)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())
