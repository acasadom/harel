"""SurrealStore contract tests, run against a real SurrealDB server in the stack.

The unit tests run against the in-process `mem://` engine, but each `mem://`
connection is isolated, so genuine multi-connection concurrency is validated here
against a real SurrealDB server (ws://), as the compose `test` service when
STM_STORE_BACKEND=surrealdb (skipped otherwise). They cover the store contract
directly — version/CAS conflict (the THROW-gated transaction), transactional
outbox, dedupe — which the pipeline test exercises only along the happy path.
Unique (uuid) execution ids isolate runs on the shared database.
"""

import os

import pytest

from harel.engine.execution import Execution
from harel.engine.store import StoreConflict, TimerOp
from harel.spec.states import Event

pytestmark = pytest.mark.stack


@pytest.fixture
def store():
    if os.environ.get("STM_STORE_BACKEND") != "surrealdb":
        pytest.skip("not the surrealdb backend")
    url = os.environ.get("STM_SURREAL_URL")
    if not url:
        pytest.skip("STM_SURREAL_URL not set")
    from harel.engine.store import SurrealStore

    s = SurrealStore.from_url(
        url,
        namespace=os.environ.get("STM_SURREAL_NS", "harel"),
        database=os.environ.get("STM_SURREAL_DB", "harel"),
        username=os.environ.get("STM_SURREAL_USER"),
        password=os.environ.get("STM_SURREAL_PASS", ""),
    )
    yield s
    s.close()


def test_first_save_inserts_and_bumps_version(store):
    e = Execution(definition_id="d", context={"n": 1})
    store.save(e)
    assert e.version == 1
    loaded = store.load(e.id)
    assert loaded is not None and loaded.version == 1 and loaded.context == {"n": 1}


def test_sequential_saves_increment_version(store):
    e = Execution(definition_id="d")
    store.save(e)
    store.save(e)
    assert e.version == 2
    assert store.load(e.id).version == 2


def test_stale_write_raises_conflict(store):
    e = Execution(definition_id="d")
    store.save(e)  # version -> 1

    other = store.load(e.id)  # a second view at version 1
    other.context["w"] = "won"
    store.save(other)  # commits version 2

    e.context["w"] = "stale"
    with pytest.raises(StoreConflict) as exc:
        store.save(e)
    assert exc.value.expected == 1 and exc.value.found == 2
    assert e.version == 1
    assert store.load(e.id).context["w"] == "won"


def test_commit_outbox_and_dedupe(store):
    e = Execution(definition_id="d")
    store.commit(e, [("parent-x", Event(kind="Finished"))], processed_event_id="evt-1")

    pending = [p for p in store.pending_outbox() if p.target_id == "parent-x"]
    assert len(pending) == 1 and pending[0].event.kind == "Finished"
    assert store.is_processed(e.id, "evt-1")
    assert not store.is_processed(e.id, "nope")

    store.ack_outbox(pending[0].seq)
    assert all(p.target_id != "parent-x" for p in store.pending_outbox())


def test_timers_schedule_and_cancel(store):
    e = Execution(definition_id="d")
    store.commit(e, [], timers=(TimerOp("schedule", "Fork.A", fire_at=100.0),))
    due = [t for t in store.due_timers(150.0) if t[0] == e.id]
    assert due == [(e.id, "Fork.A", 100.0)]
    store.commit(e, [], timers=(TimerOp("cancel", "Fork.A"),))
    assert [t for t in store.due_timers(150.0) if t[0] == e.id] == []
