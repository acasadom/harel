"""`harel.lsp.symbols` — the symbol index + cursor resolution behind hover,
go-to-definition and completion. Pure (no pygls)."""

from harel.lsp.symbols import import_at, index, resolve_state, scope_at, word_at

SRC = """event Go { status: string  n: int }
guard ok = status == "x"
machine M {
  initial A
  state A { on enter mod.f }
  state B {}
  from A to B on Go where ok
}
"""


def test_index_collects_declarations_with_positions():
    idx = index(SRC)
    assert (idx.events["Go"].line, idx.events["Go"].column) == (1, 1)
    assert (idx.guards["ok"].line, idx.guards["ok"].column) == (2, 1)
    assert (idx.states["A"][0].line, idx.states["A"][0].column) == (5, 3)
    assert (idx.states["B"][0].line, idx.states["B"][0].column) == (6, 3)


def test_word_at_resolves_word_and_category():
    # line 6 (0-based): "  from A to B on Go where ok"
    assert word_at(SRC, 6, 7).word == "A" and word_at(SRC, 6, 7).category == "state"
    assert word_at(SRC, 6, 12).word == "B" and word_at(SRC, 6, 12).category == "state"
    assert word_at(SRC, 6, 17).word == "Go" and word_at(SRC, 6, 17).category == "event"
    assert word_at(SRC, 6, 26).word == "ok" and word_at(SRC, 6, 26).category == "guard"


def test_word_at_off_a_token_is_none():
    assert word_at(SRC, 6, 0) is None  # leading whitespace
    assert word_at(SRC, 99, 0) is None  # past EOF


def test_word_at_on_event_list_continuation():
    src = "machine M { initial A  state A {}  from A to A on Go | Stop }\n"
    col = src.index("Stop")
    ref = word_at(src, 0, col)
    assert ref.word == "Stop" and ref.category == "event"


def test_word_at_quoted_name_uses_inner_text():
    src = 'machine M {\n  initial Start\n  state "Place Order" {}\n  from Start to "Place Order" on Go\n}\n'
    # cursor inside the quoted reference on the `from` line (line index 3)
    line = 3
    col = src.splitlines()[line].index("Place")
    ref = word_at(src, line, col)
    assert ref.word == "Place Order" and ref.category == "state"


def test_lookup_hover_detail():
    idx = index(SRC)
    assert "state A" in idx.lookup("A", "state").detail
    assert "on enter" in idx.lookup("A", "state").detail
    assert "fields: status: string" in idx.lookup("Go", "event").detail


def test_lookup_respects_category():
    idx = index(SRC)
    assert idx.lookup("Go", "state") is None  # Go is an event, not a state
    assert idx.lookup("Go", "event") is not None


def test_completion_names_by_category():
    idx = index(SRC)
    assert {s.name for s in idx.names("event")} == {"Go"}
    assert {s.name for s in idx.names("state")} >= {"A", "B"}
    assert {s.name for s in idx.names(None)} >= {"Go", "ok", "A", "B"}


def test_parse_error_yields_empty_index():
    idx = index("machine M { initial }")  # malformed
    assert idx.states == {} and idx.events == {}


# --- cross-file (imports) -----------------------------------------------------

LIB = "event Lib_evt { n: int }\nfragment Frag(x: value) {\n  state S {}\n}\n"


def test_imported_declarations_carry_their_file_uri(tmp_path):
    (tmp_path / "lib.stm").write_text(LIB)
    main = 'import "lib.stm"\nmachine M {\n  initial A\n  state A {}\n  use Frag(x = 1) as F\n}\n'
    idx = index(main, base_path=tmp_path, uri=(tmp_path / "main.stm").as_uri())
    # the imported fragment + event resolve, tagged with the lib's uri
    assert idx.fragments["Frag"].uri == (tmp_path / "lib.stm").as_uri()
    assert idx.fragments["Frag"].line == 2
    assert idx.events["Lib_evt"].uri == (tmp_path / "lib.stm").as_uri()


def test_aliased_import_namespaces_fragment_keys(tmp_path):
    (tmp_path / "lib.stm").write_text(LIB)
    main = 'import "lib.stm" as r\nmachine M {\n  initial A\n  state A {}\n  use r.Frag(x = 1) as F\n}\n'
    idx = index(main, base_path=tmp_path, uri=(tmp_path / "main.stm").as_uri())
    assert "r.Frag" in idx.fragments
    assert idx.fragments["r.Frag"].uri == (tmp_path / "lib.stm").as_uri()
    # a dotted reference (`use r.Frag`) is one word, resolved as a fragment
    line = "machine M { initial A  state A {}  use r.Frag(x = 1) as F }"
    ref = word_at(line, 0, line.index("r.Frag"))
    assert ref.word == "r.Frag" and ref.category == "fragment"


def test_import_at_detects_path_and_alias():
    row = 'import "review.stm" as jobs'
    assert import_at(row, 0, 10) == "review.stm"  # cursor in the path string
    assert import_at(row, 0, row.index("jobs")) == "review.stm"  # cursor on the alias
    assert import_at(row, 0, 2) is None  # on the `import` keyword, not a target
    assert import_at('import "lib.stm"', 0, 9) == "lib.stm"  # no alias
    assert import_at("machine M {}", 0, 3) is None  # not an import line


def test_invoke_fqn_resolves_to_imported_machine(tmp_path):
    (tmp_path / "review.stm").write_text("machine review {\n  initial D\n  final D success\n}\n")
    main = (
        'import "review.stm" as jobs\n'
        "machine M {\n  initial Run\n  state Run { invoke jobs.review }\n"
        "  final Done success\n  from Run to Done on Returned\n}\n"
    )
    idx = index(main, base_path=tmp_path, uri=(tmp_path / "main.stm").as_uri())
    assert "jobs.review" in idx.machines
    # the dotted FQN after `invoke` is one word, categorised as an invoke target
    line = "  state Run { invoke jobs.review }"
    ref = word_at(line, 0, line.index("jobs.review"))
    assert ref.word == "jobs.review" and ref.category == "invoke"
    # go-to-definition lands on `machine review` in the imported file
    sym = idx.lookup("jobs.review", "invoke")
    assert sym is not None and sym.uri == (tmp_path / "review.stm").as_uri() and sym.line == 1


def test_local_declaration_overrides_an_imported_one(tmp_path):
    (tmp_path / "lib.stm").write_text("event Shared { n: int }\n")
    main = 'import "lib.stm"\nevent Shared { m: string }\nmachine M { initial A  state A {} }\n'
    idx = index(main, base_path=tmp_path, uri=(tmp_path / "main.stm").as_uri())
    # the local Shared (line 2, no uri) wins over the imported one
    assert idx.events["Shared"].line == 2
    assert idx.events["Shared"].uri == (tmp_path / "main.stm").as_uri()


# --- scope-aware state resolution ---------------------------------------------

REPEATED = """machine M {
  initial Outer
  state Outer {
    initial Step
    state Step {}
    final Done success
    from Step to Done on Go
  }
  final Done success
  from Outer to Done on Fin
}
"""


def test_scope_at_tracks_enclosing_state_blocks():
    assert scope_at(REPEATED, 6, 4) == ("M", "Outer")  # inside `state Outer { ... }`
    assert scope_at(REPEATED, 9, 2) == ("M", "")  # back at the machine root


def test_resolve_state_picks_the_in_scope_declaration():
    idx = index(REPEATED)
    # "Done" inside Outer resolves to the inner final (line 6)...
    inner_col = REPEATED.splitlines()[6].index("Done")
    assert resolve_state(idx, REPEATED, 6, inner_col, "Done").line == 6
    # ...while "Done" at the root resolves to the outer final (line 9)
    outer_col = REPEATED.splitlines()[9].index("Done")
    assert resolve_state(idx, REPEATED, 9, outer_col, "Done").line == 9


def test_resolve_state_unknown_scope_returns_none():
    # a name not resolvable from the scope -> None (caller falls back to first match)
    idx = index(REPEATED)
    assert resolve_state(idx, REPEATED, 6, 4, "Nope") is None
