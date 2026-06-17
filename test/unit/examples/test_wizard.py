"""Headless spec for the durable-wizard example machine (examples/nicegui_wizard).

Drives the wizard through the `DurableRunner` over an in-memory store — no NiceGUI —
so CI guards the step sequence, the advance guards, and the durable context. The
NiceGUI app (`app.py`) is the same machine with a UI bolted on.
"""

from pathlib import Path

import pytest

from harel import DictStore, DurableRunner, Event, definition_from_dsl_file, validate_or_raise

WIZARD_STM = Path(__file__).parents[3] / "examples" / "nicegui_wizard" / "wizard.stm"


@pytest.fixture
def runner() -> DurableRunner:
    defn = definition_from_dsl_file(WIZARD_STM, "wizard")
    validate_or_raise(defn)
    return DurableRunner(DictStore(), {defn.id: defn})


def _defn_id(runner: DurableRunner) -> str:
    return next(iter(runner.definitions))


def test_happy_path_reaches_done(runner: DurableRunner) -> None:
    exe = runner.create(_defn_id(runner))
    assert exe.active_path == "Account"

    exe = runner.process(exe.id, Event(kind="Next", data={"email": "a@b.c"}))
    assert exe.active_path == "Profile"
    exe = runner.process(exe.id, Event(kind="Next", data={"full_name": "Ada"}))
    assert exe.active_path == "Verify"
    exe = runner.process(exe.id, Event(kind="Verified"))

    assert exe.active_path == "Done"
    assert exe.status.name == "DONE"
    assert exe.outcome == "success"
    # the typed fields were persisted into the (durable) context
    assert exe.context["email"] == "a@b.c"
    assert exe.context["full_name"] == "Ada"


def test_guard_blocks_empty_field(runner: DurableRunner) -> None:
    exe = runner.create(_defn_id(runner))
    # account_ok = email != "" — an empty email cannot advance
    exe = runner.process(exe.id, Event(kind="Next", data={"email": ""}))
    assert exe.active_path == "Account"
    exe = runner.process(exe.id, Event(kind="Next", data={"email": "x@y.z"}))
    assert exe.active_path == "Profile"


def test_back_preserves_typed_data(runner: DurableRunner) -> None:
    exe = runner.create(_defn_id(runner))
    exe = runner.process(exe.id, Event(kind="Next", data={"email": "a@b.c"}))
    exe = runner.process(exe.id, Event(kind="Next", data={"full_name": "Ada"}))
    assert exe.active_path == "Verify"

    exe = runner.process(exe.id, Event(kind="Back"))
    assert exe.active_path == "Profile"
    # going back does not wipe what was typed; it stays in the context
    assert exe.context["email"] == "a@b.c"
    assert exe.context["full_name"] == "Ada"


def test_resume_from_store_after_reload(runner: DurableRunner) -> None:
    """A fresh runner over the SAME store resumes on the reached step (the restart)."""
    store = DictStore()
    defn = definition_from_dsl_file(WIZARD_STM, "wizard")
    r1 = DurableRunner(store, {defn.id: defn})
    exe = r1.create(defn.id)
    exe = r1.process(exe.id, Event(kind="Next", data={"email": "a@b.c"}))
    assert exe.active_path == "Profile"

    # simulate a server restart: brand-new runner, same store, load by id
    r2 = DurableRunner(store, {defn.id: defn})
    resumed = store.load(exe.id)
    assert resumed is not None
    assert resumed.active_path == "Profile"
    assert resumed.context["email"] == "a@b.c"
    resumed = r2.process(resumed.id, Event(kind="Next", data={"full_name": "Ada"}))
    assert resumed.active_path == "Verify"
