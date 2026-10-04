"""The stable API for writing an `ExecutionStore` backend outside harel.

Everything a backend needs — the protocols it implements, the records it stores and returns,
the errors it raises, and the helpers the built-in backends share — under names that are part
of harel's public API (unlike the underscored ones in `harel.engine.store._base`, which may
change). Check a backend against the protocol with `harel.testing`.

- `ExecutionStore` / `AsyncExecutionStore` — the protocols (sync / async).
- `Step` — the description of a commit every writer passes to `commit(..., step=)`;
- `OutboxEntry`, `SpawnEntry`, `TimerOp` — what `pending_outbox`, `pending_spawns` return and
  `commit` receives; `ExecutionSummary`, `ExecutionPage` — what `list_executions` returns.
- `StoreConflict` — `commit`/`save` lost the version CAS; `ExecutionAlreadyExists`.
- `encode_offset(offset)` / `decode_offset(cursor)` — an opaque pagination cursor over an
  integer offset (a missing or garbled cursor decodes to 0).
- `matches(summary, status, definition_id, roots_only)` — the `list_executions` filter, for a
  backend that filters client-side.
- `listing_page(rows, limit, offset)` — a page from the rows of a listing query that fetched
  `limit + 1` rows, each `(id, definition_id, version, status, outcome, active_path,
  parent_id, finished_at)`.
- `like_prefix(prefix)` — `prefix` escaped for `LIKE ? ESCAPE '\\'` plus a trailing `%`, for
  `ids_with_prefix` (so `_`, `%` and `\\` in an id match literally).
- `DEFAULT_TRACE_MAX` — the trace ring's default size.
"""

from harel.engine.aio_store._base import AsyncExecutionStore
from harel.engine.execution import ExecutionPage, ExecutionSummary
from harel.engine.store._base import (
    DEFAULT_TRACE_MAX,
    ExecutionAlreadyExists,
    ExecutionStore,
    OutboxEntry,
    SpawnEntry,
    Step,
    StoreConflict,
    TimerOp,
)
from harel.engine.store._base import _decode_offset as decode_offset
from harel.engine.store._base import _encode_offset as encode_offset
from harel.engine.store._base import _like_prefix as like_prefix
from harel.engine.store._base import _listing_page as listing_page
from harel.engine.store._base import _matches as matches

__all__ = [
    "AsyncExecutionStore",
    "DEFAULT_TRACE_MAX",
    "ExecutionAlreadyExists",
    "ExecutionPage",
    "ExecutionStore",
    "ExecutionSummary",
    "OutboxEntry",
    "SpawnEntry",
    "StoreConflict",
    "Step",
    "TimerOp",
    "decode_offset",
    "encode_offset",
    "like_prefix",
    "listing_page",
    "matches",
]
