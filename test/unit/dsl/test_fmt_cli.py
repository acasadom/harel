"""The `harel-fmt` CLI (`harel.fmt._run`): write / --check / --diff."""

from harel.fmt import _run

MESSY = "machine M {\n    initial A\n    state A {}\n}\n"
CANONICAL = "machine M {\n  initial A\n  state A {}\n}\n"


def test_writes_in_place_by_default(tmp_path, capsys):
    f = tmp_path / "m.stm"
    f.write_text(MESSY)
    assert _run([str(f)]) == 0
    assert f.read_text() == CANONICAL
    assert "formatted" in capsys.readouterr().out


def test_already_canonical_is_a_noop(tmp_path, capsys):
    f = tmp_path / "m.stm"
    f.write_text(CANONICAL)
    assert _run([str(f)]) == 0
    assert capsys.readouterr().out == ""  # nothing printed when unchanged


def test_check_does_not_write_and_signals_change(tmp_path):
    f = tmp_path / "m.stm"
    f.write_text(MESSY)
    assert _run([str(f), "--check"]) == 2
    assert f.read_text() == MESSY  # untouched


def test_check_clean_returns_zero(tmp_path):
    f = tmp_path / "m.stm"
    f.write_text(CANONICAL)
    assert _run([str(f), "--check"]) == 0


def test_diff_prints_without_writing(tmp_path, capsys):
    f = tmp_path / "m.stm"
    f.write_text(MESSY)
    assert _run([str(f), "--diff"]) == 0
    out = capsys.readouterr().out
    assert "--- " in out and "+++ " in out
    assert f.read_text() == MESSY  # untouched
