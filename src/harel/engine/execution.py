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

    # --- orthogonal (model (i)) ---------------------------------------------
    parent_id: Optional[str] = None  # set on a child: the parent Execution to notify
    child_id: Optional[str] = None  # set on a child: its key in the parent's `children`
    children: dict[str, ChildState] = {}  # parent: child_id -> ChildState (join counter)
    invoke_seq: dict[str, int] = {}  # invoke-state path -> times entered (deterministic child ids,
    #                                  so re-invoking the same state in a loop spawns a fresh child)
