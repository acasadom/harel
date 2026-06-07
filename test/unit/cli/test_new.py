"""`harel new` — the scaffold command. The starter machine it emits must validate and
run out of the box (no actions/binding), so a newcomer goes from zero to a working
machine in one command."""

from pathlib import Path

from harel import DictStore, DurableRunner, Event, definition_from_dsl_file
from harel.cli import _sanitize, main


def test_new_creates_a_file_that_validates(tmp_path, capsys):
    path = tmp_path / "flow.stm"
    rc = main(["new", str(path)])
    assert rc == 0 and path.exists()
    assert "created" in capsys.readouterr().out
    # the scaffold validates with no actions bound
    defn = definition_from_dsl_file(path, "flow", validate=True)
    assert defn.id == "flow"


def test_scaffold_runs_to_a_success_outcome(tmp_path):
    path = tmp_path / "flow.stm"
    main(["new", str(path)])
    defn = definition_from_dsl_file(path, "flow")
    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id)
    exe = runner.process(exe.id, Event(kind="Submit"))
    exe = runner.process(exe.id, Event(kind="Approve"))
    assert exe.status.name == "DONE" and exe.outcome == "success"


def test_name_defaults_to_file_stem_and_is_overridable(tmp_path):
    path = tmp_path / "review.stm"
    main(["new", str(path)])
    assert "machine review {" in path.read_text()

    path2 = tmp_path / "x.stm"
    main(["new", str(path2), "Custom"])
    assert "machine Custom {" in path2.read_text()


def test_sanitize_makes_a_valid_identifier():
    assert _sanitize("todo-list") == "todo_list"
    assert _sanitize("my.flow") == "my_flow"
    assert _sanitize("2fast") == "m_2fast"  # can't start with a digit
    assert _sanitize("") == "machine"


def test_refuses_to_overwrite_without_force(tmp_path, capsys):
    path = tmp_path / "keep.stm"
    path.write_text("machine keep { initial A  state A {} }")
    rc = main(["new", str(path)])
    assert rc == 1
    assert "already exists" in capsys.readouterr().err
    assert path.read_text().startswith("machine keep {")  # untouched

    rc = main(["new", str(path), "--force"])
    assert rc == 0
    assert "initial Draft" in path.read_text()  # now the scaffold


def test_minimal_example_validates():
    example = Path(__file__).parents[3] / "examples" / "minimal" / "approval.stm"
    defn = definition_from_dsl_file(example, "approval", validate=True)
    assert defn.id == "approval"
