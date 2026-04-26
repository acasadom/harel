"""Execution regression for the engine: each scenario must reproduce the trace,
context and status that the (now removed) legacy engine produced.

These EXPECTED values were captured from the legacy engine via `run_old` while it
was still present (live new-vs-legacy parity, Phases 2-4 batch 1). At the swap
(Phase 4 batch 2) the legacy engine was deleted, so they are frozen here as the
behavioural oracle the new engine (`run_new`) must keep matching.
"""

import pytest
from scenarios import SCENARIOS, run_new

# Frozen legacy outputs (per scenario: trace of per-event end_state, final
# context, final status).
EXPECTED = {
    "linear": {
        "trace": [
            {"event": "Start", "end_state": "B"},
            {"event": "Work", "end_state": "B"},
            {"event": "Exit", "end_state": "C"},
        ],
        "context": {"trace": ["enter_a", "enter_b", "activity_b", "exit_c"]},
        "status": "DONE",
    },
    "nested": {
        "trace": [
            {"event": "Start", "end_state": "Outer.In1"},
            {"event": "Step", "end_state": "Outer"},
            {"event": "Finish", "end_state": "Done"},
        ],
        "context": {"trace": ["n_in1", "n_in2", "n_done"]},
        "status": "DONE",
    },
    "scope_override": {
        "trace": [
            {"event": "Start", "end_state": "Outer.In1"},
            {"event": "Tick", "end_state": "Outer"},
        ],
        "context": {"trace": ["o_in1", "o_in2"]},
        "status": "RUNNING",
    },
    "history_resume": {
        "trace": [
            {"event": "Start", "end_state": "Outer.In1"},
            {"event": "Step", "end_state": "Outer"},
            {"event": "Pause", "end_state": "Wait"},
            {"event": "Resume", "end_state": "Outer.In1"},
        ],
        "context": {"trace": ["h_in1", "h_in2", "h_wait", "h_in1"]},
        "status": "RUNNING",
    },
    "history_restart": {
        "trace": [
            {"event": "Start", "end_state": "Outer.In1"},
            {"event": "Step", "end_state": "Outer"},
            {"event": "Pause", "end_state": "Wait"},
            {"event": "Resume", "end_state": "Outer.In1"},
        ],
        "context": {"trace": ["h_in1", "h_in2", "h_wait", "h_in1"]},
        "status": "RUNNING",
    },
    "selector_done": {
        "trace": [
            {"event": "Start", "end_state": "Decide"},
            {"event": "Go", "end_state": "Done"},
        ],
        "context": {"pick": True, "trace": ["enter_decide", "pick", "enter_done"]},
        "status": "DONE",
    },
    "selector_retry": {
        "trace": [
            {"event": "Start", "end_state": "Decide"},
            {"event": "Go", "end_state": "Decide"},
        ],
        "context": {"pick": False, "trace": ["enter_decide", "pick", "enter_retry", "enter_decide"]},
        "status": "RUNNING",
    },
    "bubble": {
        "trace": [{"event": "Start", "end_state": "Done"}],
        "context": {"trace": ["b_in1", "b_inend", "b_done"]},
        "status": "DONE",
    },
    "cancel": {
        "trace": [
            {"event": "Start", "end_state": "Wait"},
            {"event": "Cancel", "end_state": "Wait"},
        ],
        "context": {"trace": ["enter_b"]},
        "status": "CANCELLED",
    },
    "reset": {
        "trace": [
            {"event": "Start", "end_state": "B"},
            {"event": "Work", "end_state": "B"},
            {"event": "Reset", "end_state": "B"},
        ],
        "context": {"trace": ["enter_a", "enter_b"]},
        "status": "RUNNING",
    },
    "set_state": {
        "trace": [
            {"event": "Start", "end_state": "B"},
            {"event": "SetState", "end_state": "C"},
        ],
        "context": {"trace": ["enter_a", "enter_b", "exit_c"]},
        "status": "DONE",
    },
    "orthogonal_join": {
        "trace": [
            {"event": "Start", "end_state": "Fork"},
            {"event": "Go", "end_state": "Done"},
        ],
        "context": {"trace": ["f_done"]},
        "status": "DONE",
    },
    "orthogonal_pending": {
        "trace": [
            {"event": "Start", "end_state": "Fork"},
            {"event": "GoA", "end_state": "Fork"},
            {"event": "GoB", "end_state": "Done"},
        ],
        "context": {"trace": ["f_done"]},
        "status": "DONE",
    },
}


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s["name"])
def test_engine_parity(scenario):
    assert run_new(scenario) == EXPECTED[scenario["name"]]
