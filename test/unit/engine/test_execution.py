"""Unit tests for the serializable `Execution` model.

The Execution is pure data (the engine reads it and mutates it; runners persist
it). These guard the defaults and a full serialization round-trip — the latter
is what makes durable runners possible (Phase 5).
"""

from harel.engine.execution import ChildState, Execution, Status


def test_defaults():
    e = Execution(definition_id="d")
    assert e.status is Status.PENDING
    assert e.root_path == ""
    assert e.active_path is None
    assert e.history == {}
    assert e.context == {}
    assert e.children == {}
    assert e.processed_events == 0
    assert e.parent_id is None
    assert e.child_id is None
    assert e.version == 0
    assert e.id  # a uuid was assigned


def test_ids_are_unique():
    assert Execution(definition_id="d").id != Execution(definition_id="d").id


def test_childstate_defaults_to_unfinished():
    assert ChildState(root_path="R.A").finished is False


def test_status_values():
    assert {s.value for s in Status} == {
        "PENDING",
        "RUNNING",
        "DONE",
        "CANCELLED",
        "SUSPENDED",
        "CANCELLING",
        "FAILED",
    }


def test_json_round_trip_preserves_everything():
    e = Execution(
        definition_id="d",
        root_path="R",
        status=Status.RUNNING,
        active_path="R.x",
        history={"R": "R.x"},
        context={"n": 1, "items": ["a", "b"]},
        processed_events=3,
        parent_id="p",
        child_id="c",
        children={"c1": ChildState(root_path="R.A", finished=True)},
    )
    again = Execution.model_validate_json(e.model_dump_json())
    assert again == e
    assert again.status is Status.RUNNING
    assert again.children["c1"].finished is True
    assert again.context["items"] == ["a", "b"]
