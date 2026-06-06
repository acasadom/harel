"""Behavioural tests for the `harel` CLI (invoke `main(argv)` directly)."""

from pathlib import Path

import pytest

from harel.cli import main

DATA = Path(__file__).parents[2] / "data"
ORDER = str(DATA / "order.stm")


def test_validate_ok(capsys):
    rc = main(["validate", ORDER])
    out = capsys.readouterr().out
    assert rc == 0
    assert "ok" in out


def test_validate_reports_errors(tmp_path, capsys):
    broken = tmp_path / "broken.stm"
    broken.write_text("machine m {\n  initial A\n  state A {}\n  state Sink {}\n  from A to Sink on Go\n}\n")
    rc = main(["validate", str(broken)])
    out = capsys.readouterr().out
    assert rc == 1  # Sink is an execution-ending terminal with no outcome
    assert "terminal" in out.lower()


def test_render_plantuml(capsys):
    rc = main(["render", ORDER])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Delivered" in out and "-->" in out


def test_render_mermaid(capsys):
    rc = main(["render", ORDER, "--mermaid"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "stateDiagram-v2" in out


def test_list(capsys):
    rc = main(["list", ORDER])
    out = capsys.readouterr().out
    assert rc == 0
    assert "machines:" in out and "order" in out
    assert "CarrierUpdate" in out  # one of the declared events


def test_run_drives_events(tmp_path, capsys):
    gate = tmp_path / "gate.stm"
    gate.write_text(
        "machine gate {\n"
        "  initial Closed\n"
        "  state Closed {}\n"
        "  state Open {}\n"
        "  from Closed to Open on Unlock\n"
        "  from Open to Closed on Lock\n"
        "}\n"
    )
    rc = main(["run", str(gate), "-e", "Unlock", "-e", "Lock"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "(start)" in out and "Open" in out and "Closed" in out
    assert "status: RUNNING" in out


def test_run_with_event_data(tmp_path, capsys):
    m = tmp_path / "guarded.stm"
    m.write_text(
        "machine m {\n"
        "  initial A\n"
        "  state A {}\n"
        "  final Done success {}\n"
        '  from A to Done on Go where status == "ok"\n'
        "}\n"
    )
    rc = main(["run", str(m), "-e", 'Go:{"status": "ok"}'])
    out = capsys.readouterr().out
    assert rc == 0
    assert "outcome: success" in out


def test_fmt_check_on_formatted_file(capsys):
    # the corpus files are canonically formatted, so --check passes (rc 0)
    rc = main(["fmt", "--check", ORDER])
    assert rc == 0


def test_version_exits_zero(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert "harel" in capsys.readouterr().out


def test_unknown_file_is_a_clean_error(capsys):
    rc = main(["validate", "does-not-exist.stm"])
    assert rc == 1
    assert "error" in capsys.readouterr().err.lower()
