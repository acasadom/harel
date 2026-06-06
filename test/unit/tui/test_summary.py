"""Pure formatting helpers (no textual)."""

from harel.engine.execution import ExecutionSummary, Status
from harel.tui import summary


def test_every_status_has_a_glyph_and_colour():
    for status in Status:
        assert summary.status_glyph(status)  # non-empty
        assert summary.status_color(status)
        assert status.value in summary.status_label(status)


def test_short_path():
    assert summary.short_path(None) == "—"
    assert summary.short_path("A.B") == "A.B"
    long = "Root.Branch.Composite.Inner.Leaf" * 3
    out = summary.short_path(long, width=20)
    assert len(out) <= 20 and out.startswith("…") and long.endswith(out[1:])


def test_truncate_is_bounded_and_single_line():
    out = summary.truncate({"a": "x" * 200}, width=30)
    assert len(out) <= 30 and "\n" not in out and out.endswith("…")
    assert summary.truncate("short") == "short"


def test_verdict_prefers_outcome():
    s = ExecutionSummary(id="e", definition_id="d", status=Status.DONE, outcome="success")
    assert summary.verdict(s) == "success"
    assert summary.verdict(ExecutionSummary(id="e", definition_id="d", status=Status.RUNNING)) == "—"
