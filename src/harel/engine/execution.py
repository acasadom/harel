"""The **Execution**: the serializable running instance of a `Definition`.

Pure data, no logic and no IO. Holds the active configuration plus the user
context. The engine reads it and returns a new one; runners persist it. The
active position is addressed by `active_path` (a stable `full_path`), not by
object references — references live in the `Definition`.

Orthogonal (model (i)): an AND-state runs each region as a **separate child
`Execution`** over the *same* `Definition` but with a different `root_path` (the
branch node). The parent tracks its children in `children` and joins when they
all emit `Finished`; a child carries `parent_id`/`child_id` so the runner can
route its `Finished` back. There is no shared memory between Executions — they
communicate only by events (the parent's `context` is independent of each
child's).
"""

from __future__ import annotations

import uuid
from enum import Enum
from typing import Optional

import pydantic

from harel.spec.states import Event


class Status(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    CANCELLED = "CANCELLED"
    SUSPENDED = "SUSPENDED"  # paused by the control plane; state + queue preserved, resume -> RUNNING
    CANCELLING = "CANCELLING"  # cooperative cancel in flight: the worker drains the backlog until
    #                            the injected Cancel event reaches the machine's own cleanup transition
    FAILED = "FAILED"  # the runtime aborted it on an UNHANDLED action error (a bug) — distinct from DONE
    #                    (reached a modelled terminal); the captured `error` is the dead-letter record


def _new_id() -> str:
    return uuid.uuid4().hex


class ChildState(pydantic.BaseModel):
    """A region spawned by an orthogonal parent: its branch `root_path`, whether
    it has reported `Finished` (the join counter), and the result it carried on
    that `Finished` (its terminal `outcome` + the `carry`-projected context)."""

    root_path: str
    finished: bool = False
    outcome: Optional[str] = None  # the region's terminal outcome, carried on its `Finished`
    result: dict = {}  # the region's `carry`-projected context, carried on its `Finished`
    submachine: bool = False  # True for an `invoke` child (a different Definition; not broadcast to)
    key: str = ""  # stable region key for `region_results` (decoupled from the seq'd child id)


class Execution(pydantic.BaseModel):
    id: str = pydantic.Field(default_factory=_new_id)
    definition_id: str
    definition_fqn: Optional[str] = None  # set on a submachine child: its FQN, so any
    #                                       worker can (re)resolve its Definition lazily
    root_path: str = ""  # node this execution runs (orthogonal child => branch node)
    status: Status = Status.PENDING
    outcome: Optional[str] = None  # result label of the terminal reached (model's, e.g. "failed");
    #                                None = plain completion. Orthogonal to `status` (lifecycle).
    error: Optional[str] = None  # set with status FAILED: the unhandled action exception (type: message)
    active_path: Optional[str] = None  # full_path of the active leaf (None before start)
    history: dict = {}  # composite_path -> last active child path
    context: dict = {}  # global user execution context
    processed_events: int = 0
    version: int = 0  # optimistic-concurrency token: a save commits version+1 iff the
    #                   stored row is still at `version` (single-writer per Execution)

    priority: int = pydantic.Field(
        default=0, ge=0, le=4
    )  # 0=normal … 4=highest; controls worker claim weighting

    # --- orthogonal (model (i)) ---------------------------------------------
    parent_id: Optional[str] = None  # set on a child: the parent Execution to notify
    child_id: Optional[str] = None  # set on a child: its key in the parent's `children`
    deferred: list[Event] = []  # events held by a `defer` state, re-delivered on entering a handler
    children: dict[str, ChildState] = {}  # parent: child_id -> ChildState (join counter)
    invoke_seq: dict[str, int] = {}  # spawn-site path -> times entered (invoke / orthogonal /
    #                                  fan-out): the per-entry seq in the deterministic child ids,
    #                                  so re-entering the same state in a loop spawns fresh children


class ExecutionSummary(pydantic.BaseModel):
    """A lightweight projection of an `Execution` for list/monitor views: every field is
    cheap to extract and renders a row without loading the heavy `context`/`history`/
    `children`. Drill-down loads the full `Execution` via `store.load(id)`. Produced by
    `ExecutionStore.list_executions`."""

    id: str
    definition_id: str
    status: Status
    outcome: Optional[str] = None
    active_path: Optional[str] = None
    version: int = 0
    parent_id: Optional[str] = None  # None => a root; set => an orthogonal region / invoke child

    @classmethod
    def of(cls, exe: "Execution") -> "ExecutionSummary":
        """Project a full Execution (the path used by backends holding live objects)."""
        return cls(
            id=exe.id,
            definition_id=exe.definition_id,
            status=exe.status,
            outcome=exe.outcome,
            active_path=exe.active_path,
            version=exe.version,
            parent_id=exe.parent_id,
        )

    @classmethod
    def from_data(cls, raw: dict, version: int) -> "ExecutionSummary":
        """Project from a parsed Execution-JSON dict (the path used by backends that store
        the Execution as an opaque JSON blob `data` plus a broken-out `version` column)."""
        return cls(
            id=raw["id"],
            definition_id=raw["definition_id"],
            status=raw["status"],
            outcome=raw.get("outcome"),
            active_path=raw.get("active_path"),
            version=version,
            parent_id=raw.get("parent_id"),
        )


class ExecutionPage(pydantic.BaseModel):
    """One page of `list_executions`: up to `limit` summaries plus an opaque `next_cursor`
    to fetch the following page (None => no more). The cursor is backend-specific (an
    offset, a SCAN cursor, a DynamoDB LastEvaluatedKey) and must be treated as opaque."""

    items: list[ExecutionSummary] = []
    next_cursor: Optional[str] = None
