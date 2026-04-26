"""Machine resolvers — the `invoke` FQN -> Definition seam: in-memory (Dict),
disk (File), Python module (Module), and an arbitrary source loader (Source, the
generic database case). Convention: the last FQN segment is the machine name.
"""

from pathlib import Path

import pytest

from harel.dsl import definition_from_dsl, definition_from_dsl_file
from harel.dsl.resolve import FileResolver, ModuleResolver, SourceResolver
from harel.engine.durable import DurableRunner
from harel.engine.resolve import DictResolver, ResolveError
from harel.engine.store import DictStore

ONE = "machine worker { initial A  final A success }"
DATA = Path(__file__).parents[2] / "data"


def test_dict_resolver_registers_and_resolves():
    defn = definition_from_dsl(ONE, "worker")
    r = DictResolver({"acme.worker": defn})
    assert r.resolve("acme.worker") is defn
    r.register("acme.other", defn)
    assert r.resolve("acme.other") is defn


def test_dict_resolver_unknown_raises():
    with pytest.raises(ResolveError, match="no machine registered"):
        DictResolver({}).resolve("nope")


def test_file_resolver_maps_fqn_to_path_and_builds(tmp_path):
    (tmp_path / "acme" / "jobs").mkdir(parents=True)
    (tmp_path / "acme" / "jobs" / "worker.stm").write_text(ONE)
    r = FileResolver(tmp_path)
    defn = r.resolve("acme.jobs.worker")
    assert defn.get("A").outcome == "success"
    assert r.resolve("acme.jobs.worker") is defn  # cached


def test_file_resolver_missing_raises(tmp_path):
    with pytest.raises(ResolveError, match="no `.stm`"):
        FileResolver(tmp_path).resolve("a.b.missing")


def test_source_resolver_builds_from_loader():
    sources = {"db.worker": ONE}
    r = SourceResolver(sources.get)
    assert r.resolve("db.worker").get("A").outcome == "success"


def test_source_resolver_missing_raises():
    with pytest.raises(ResolveError, match="no source"):
        SourceResolver(lambda fqn: None).resolve("db.gone")


def test_import_registers_imported_machines_as_submachines():
    # `import "review.stm" as jobs` makes machine `review` resolvable as `jobs.review`
    defn = definition_from_dsl_file(DATA / "invoke.stm", "approval")
    assert "jobs.review" in defn.submachines
    assert defn.get("Run").invoke == "jobs.review"


def test_invoke_of_imported_machine_resolves_without_external_resolver():
    # end-to-end: the parent invokes a machine defined in an imported file, and the
    # DurableRunner resolves it from the registered submachines (no resolver= passed)
    parent = definition_from_dsl_file(DATA / "invoke.stm", "approval")
    runner = DurableRunner(DictStore(), {parent.id: parent})  # deliberately no resolver
    exe = runner.create(parent.id, context={"approved": True, "points": 70})
    final = runner.store.load(exe.id)
    assert final.active_path == "Approved"  # success + score 70 >= 50
    assert final.outcome == "success"


def test_module_resolver_reads_a_definition_attribute():
    import scenarios  # test-root module, importable by bare name

    scenarios._sub_defn = definition_from_dsl(ONE, "worker")  # type: ignore[attr-defined]
    try:
        assert ModuleResolver().resolve("scenarios._sub_defn").get("A").outcome == "success"
    finally:
        del scenarios._sub_defn  # type: ignore[attr-defined]


def test_module_resolver_reads_a_source_string_attribute():
    import scenarios

    # the attribute is a `.stm` source string -> built (machine name = last segment)
    scenarios.worker = ONE  # type: ignore[attr-defined]
    try:
        assert ModuleResolver().resolve("scenarios.worker").get("A").outcome == "success"
    finally:
        del scenarios.worker  # type: ignore[attr-defined]
