"""Scoped structured-logging context.

Replacement for the formerly external ``flog.context.contextvars`` helper.

Binds the given key/value pairs to structlog's context-local storage for the
duration of the ``with`` block, so every log line emitted inside it (including
ones produced by user-supplied state actions that have no access to the state
machine logger) carries those fields, e.g. ``suite_id``, ``state``, ``job_id``,
``run_id``. The previous context is restored on exit, which keeps nested
``with`` blocks (composite state enter -> child state enter) correct.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import structlog


@contextmanager
def contextvars(**kwargs: Any) -> Iterator[None]:
    """Bind ``kwargs`` to the structlog context for the duration of the block."""
    tokens = structlog.contextvars.bind_contextvars(**kwargs)
    try:
        yield
    finally:
        structlog.contextvars.reset_contextvars(**tokens)
