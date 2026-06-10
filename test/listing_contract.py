"""Shared `ExecutionStore.list_executions` contract: seed + assertions reused by the
unit tests (in-process backends, fresh store) and the integration tests (real servers,
stack-marked, SHARED tables).

The contract: a page of lightweight `ExecutionSummary` filtered by status (OR) /
definition_id / roots_only, paginated by an opaque cursor until `next_cursor is None`.
Ordering is stable on Dict/SQL/Mongo but best-effort/unordered on Redis (SCAN)
and DynamoDB (Scan) — so the assertions compare the reassembled **set** of ids, never
per-page order, which holds for every backend.

Everything is scoped by a `ns` (namespace) prefix on the ids and definition_ids, and
every query filters by one of the two namespaced definition_ids — so the assertions are
isolated from any other executions sharing a real backend's tables (the integration case
seeds with a unique ns per run).
"""

from harel.engine.execution import Execution, Status

# (suffix, which-definition, status, parent-suffix-or-None): 3 statuses, 2 definitions,
# the last two of d2 are children (parent_id set).
SEED = [
    ("e00", "d1", Status.RUNNING, None),
    ("e01", "d1", Status.RUNNING, None),
    ("e02", "d1", Status.DONE, None),
    ("e03", "d1", Status.DONE, None),
    ("e04", "d1", Status.SUSPENDED, None),
    ("e05", "d2", Status.RUNNING, None),
    ("e06", "d2", Status.RUNNING, None),
    ("e07", "d2", Status.DONE, None),
    ("e08", "d2", Status.SUSPENDED, None),
    ("e09", "d2", Status.FAILED, None),
    ("e10", "d2", Status.RUNNING, "e05"),  # a child (orthogonal region / invoke)
    ("e11", "d2", Status.RUNNING, "e05"),
]


def seed(store, ns: str = "") -> tuple[str, str]:
    """Persist the SEED executions namespaced by `ns`; return the two definition_ids."""
    d1, d2 = f"{ns}d1", f"{ns}d2"
    defs = {"d1": d1, "d2": d2}
    for suffix, which, status, parent in SEED:
        store.save(
            Execution(
                id=f"{ns}{suffix}",
                definition_id=defs[which],
                status=status,
                parent_id=f"{ns}{parent}" if parent else None,
            )
        )
    return d1, d2


def _ids(store, *, page=4, **filters) -> set:
    """Reassemble every matching id across pages — order-agnostic, valid on all backends.
    Asserts only that the cursor terminates (per-page size is best-effort on SCAN)."""
    ids, cursor, guard = [], None, 0
    while True:
        guard += 1
        assert guard < 1000, "pagination did not terminate"
        result = store.list_executions(limit=page, cursor=cursor, **filters)
        ids += [s.id for s in result.items]
        if result.next_cursor is None:
            break
        cursor = result.next_cursor
    return set(ids)


def assert_contract(store, *, ordered: bool, ns: str = "") -> None:
    """Run the full listing contract against `store`, seeding under `ns`. Every query is
    scoped by definition_id so the assertions survive a shared backend. `ordered` enables
    the stable-id-order checks (skip for Redis/DynamoDB)."""
    d1, d2 = seed(store, ns)
    n = lambda *s: {f"{ns}{x}" for x in s}  # noqa: E731

    # definition_id (exact) — all of d1, and d1 never leaks d2
    assert _ids(store, definition_id=d1) == n("e00", "e01", "e02", "e03", "e04")

    # status filter (single + OR), scoped to d2
    assert _ids(store, definition_id=d2, status=[Status.RUNNING]) == n("e05", "e06", "e10", "e11")
    assert _ids(store, definition_id=d2, status=[Status.DONE, Status.FAILED]) == n("e07", "e09")

    # roots_only drops the two children of d2
    assert _ids(store, definition_id=d2, roots_only=True) == n("e05", "e06", "e07", "e08", "e09")

    # combined filters
    assert _ids(store, definition_id=d2, status=[Status.RUNNING], roots_only=True) == n("e05", "e06")

    # the summary carries the projected fields (and never the heavy context/history)
    one = store.list_executions(definition_id=d1, status=[Status.RUNNING], limit=1).items[0]
    assert one.definition_id == d1 and one.status == Status.RUNNING and one.parent_id is None
    assert not hasattr(one, "context") and not hasattr(one, "history")

    if ordered:
        # a page (scoped to d1) is sorted by id and respects the per-page bound; pagination
        # preserves that order (SCAN-based backends guarantee neither)
        page = store.list_executions(definition_id=d1, limit=100)
        ids = [s.id for s in page.items]
        assert ids == sorted(ids)
        two = store.list_executions(definition_id=d1, limit=2)
        assert len(two.items) <= 2
        assert [s.id for s in two.items] == sorted(n("e00", "e01", "e02", "e03", "e04"))[:2]
