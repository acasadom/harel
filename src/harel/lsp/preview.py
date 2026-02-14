"""Pure DSL → Mermaid render for the editor preview — no LSP dependency.

`render_text(text)` parses the document and renders one machine to a Mermaid
`stateDiagram-v2` (the named one, else the first declared). A document that
declares only **fragments** (no machine) is rendered by wrapping the chosen
fragment in a synthetic preview machine that `use`s it with **placeholder
arguments** generated from its parameter signature — so a reusable fragment can
be visualised in isolation (with a `note` flagging that the args are stand-ins).

It does **not** run `validate` — an incomplete-but-parseable machine should still
draw (the live preview tracks the file as it is typed). Failures are returned
located so the webview can report them without crashing the diagram. Kept free of
`pygls` so it is testable in the ordinary suite; `server.py` wires it to a custom
LSP request.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Optional

from harel.definition.builder import BuildError
from harel.definition.model import Definition
from harel.dsl.loader import definition_from_dsl
from harel.dsl.parser import DslError, parse
from harel.viz.mermaid import render


@dataclass(frozen=True)
class Render:
    """The render outcome: `mermaid` set on success, else `error` (1-based
    `line`/`column` when the failure carries a source position). `machine` names
    the machine (or fragment) rendered or attempted; `is_fragment` marks a
    fragment preview, and `note` carries a human caption for the webview.
    `targets` lists every renderable thing in the document (machines, fragments
    and imported submachines) so the webview can offer a picker."""

    mermaid: Optional[str] = None
    machine: Optional[str] = None
    error: Optional[str] = None
    line: Optional[int] = None
    column: Optional[int] = None
    is_fragment: bool = False
    note: Optional[str] = None
    targets: list = field(default_factory=list)  # [{"name": str, "kind": "machine"|"fragment"|"submachine"}]


def _placeholder_args(params: list) -> tuple[list[str], list[str]]:
    """Synthetic `use` arguments (one per fragment parameter, by kind) plus the
    dummy sibling states a `state` parameter needs as a transition target. The
    values are stand-ins only — enough to build a renderable machine."""
    dummy_states: list[str] = []
    args: list[str] = []
    for name, kind in params:
        if kind == "action":
            args.append(f"{name} = preview.{name}")  # a literal path needs no bind
        elif kind == "state":
            dummy_states.append(f"  state {name} {{}}")
            args.append(f"{name} = {name}")
        elif kind == "guard":
            args.append(f'{name} = ({name} == "x")')
        elif kind == "value":
            args.append(f"{name} = 1")
        elif kind == "event":
            args.append(f"{name} = {name.capitalize()}Evt")
    return dummy_states, args


def _render_fragment(text: str, fname: str, params: list, base_path: Optional[Path]) -> Render:
    """Render a fragment by splicing it into a synthetic preview machine with
    placeholder arguments. The fragment is `use`d under its own name so it shows
    as a named composite."""
    dummy_states, args = _placeholder_args(params)
    wrap = (
        "machine __preview__ {\n"
        f"  initial {fname}\n"
        + ("\n".join(dummy_states) + "\n" if dummy_states else "")
        + f"  use {fname} ({', '.join(args)}) as {fname}\n"
        "}"
    )
    note = f"fragment '{fname}' — placeholder arguments"
    try:
        defn = definition_from_dsl(text + "\n" + wrap, "__preview__", base_path=base_path)
    except DslError as e:
        return Render(machine=fname, is_fragment=True, error=e.message, line=e.line, column=e.column)
    except BuildError as e:
        return Render(machine=fname, is_fragment=True, error=str(e))
    return Render(mermaid=render(defn), machine=fname, is_fragment=True, note=note)


def render_text(text: str, *, base_path: Optional[Path] = None, machine: Optional[str] = None) -> Render:
    """Render one DSL document to Mermaid. `machine` selects what to render — a
    machine, a fragment, or an imported submachine (by FQN) — falling back to the
    first machine, else the first fragment. The full target list rides along in
    `targets` so the editor can offer a picker."""
    try:
        prog = parse(text)
    except DslError as e:
        return Render(error=e.message, line=e.line, column=e.column)

    machines = list(prog.machines)
    fragments = list(prog.fragments)

    # build the primary machine once: both to render it and to discover the
    # imported submachines (extra picker targets)
    primary_defn: Optional[Definition] = None
    primary_err: Optional[tuple[str, Exception]] = None
    if machines:
        primary_name = machine if machine in prog.machines else machines[0]
        try:
            primary_defn = definition_from_dsl(text, primary_name, base_path=base_path)
        except (DslError, BuildError) as e:
            primary_err = (primary_name, e)
    submachines = list(primary_defn.submachines) if primary_defn else []

    targets = (
        [{"name": m, "kind": "machine"} for m in machines]
        + [{"name": f, "kind": "fragment"} for f in fragments]
        + [{"name": s, "kind": "submachine"} for s in submachines]
    )

    # an imported submachine, by FQN: render its (real) Definition directly
    if machine and primary_defn is not None and machine in primary_defn.submachines:
        return Render(mermaid=render(primary_defn.submachines[machine]), machine=machine, targets=targets)

    # an explicitly named fragment, or a fragments-only document: render a fragment
    if (machine in prog.fragments and machine not in prog.machines) or (not machines and fragments):
        fname = machine if machine in prog.fragments else fragments[0]
        r = _render_fragment(text, fname, prog.fragments[fname].get("__params__", []), base_path)
        return replace(r, targets=targets)

    if not machines:
        return Render(error="no machine or fragment declared in this document", targets=targets)

    name = machine if machine in prog.machines else machines[0]
    if primary_err is not None and primary_err[0] == name:
        err = primary_err[1]
        line = getattr(err, "line", None)
        column = getattr(err, "column", None)
        msg = getattr(err, "message", str(err))
        return Render(machine=name, error=msg, line=line, column=column, targets=targets)

    assert primary_defn is not None  # machines is non-empty and this name built cleanly
    return Render(mermaid=render(primary_defn), machine=name, targets=targets)
