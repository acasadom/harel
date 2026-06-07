"""harel — the unified command-line interface.

A single `harel` entry point wrapping the static tooling (validate, render, list),
an in-memory `run`, and the existing formatter / language server. Built on the stdlib
`argparse` (no extra dependencies).

    harel validate FILE [NAME]
    harel render   FILE [NAME] [--mermaid]
    harel list     FILE
    harel run      FILE [NAME] [-e KIND[:JSON] ...] [--seed JSON] [--validate]
    harel fmt      FILES... [--check|--diff]
    harel lsp
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

# A starter machine emitted by `harel new`: commented, and VALID + runnable with no
# actions (no binding needed) so a newcomer goes from zero to a working machine in one
# command. Teaches the essentials — initial, states, `final <Name> <outcome>`, and
# transitions that live inside the machine (`from <state> to <state> on <Event>`).
_STARTER_TEMPLATE = """\
# A starter harel machine. Next steps:
#   harel validate {file}
#   harel render   {file}            # PlantUML (add --mermaid for Mermaid)
#   harel run      {file} -e Submit -e Approve
#
# The DSL in a nutshell: declare the events, the initial state, the states, and the
# transitions (which live INSIDE the machine: `from <state> to <state> on <Event>`). A
# `final <Name> <outcome>` state is a terminal that carries a verdict.

# events are declared up front, so `validate` catches a typo'd event reference:
event Submit {{}}
event Approve {{}}
event Reject {{}}
event RequestChanges {{}}

machine {name} {{
  initial Draft

  state Draft {{}}
  state Review {{}}
  final Approved success
  final Rejected rejected

  from Draft  to Review    on Submit
  from Review to Approved  on Approve
  from Review to Rejected  on Reject
  from Review to Draft     on RequestChanges
}}
"""


def _sanitize(stem: str) -> str:
    """Turn a file stem into a valid DSL machine identifier: non-word chars -> `_`,
    and a leading digit gets an `m_` prefix (identifiers can't start with a digit)."""
    name = re.sub(r"\W", "_", stem) or "machine"
    return f"m_{name}" if name[0].isdigit() else name


def _load(file: str, name: Optional[str]):
    from harel.dsl import definition_from_dsl_file

    return definition_from_dsl_file(Path(file), name)


def _cmd_new(args: argparse.Namespace) -> int:
    path = Path(args.file)
    if path.exists() and not args.force:
        print(f"error: {path} already exists (use --force to overwrite)", file=sys.stderr)
        return 1
    name = args.name or _sanitize(path.stem)
    path.write_text(_STARTER_TEMPLATE.format(name=name, file=path))
    print(f"created {path}  (machine {name})")
    print("next:")
    print(f"  harel validate {path}")
    print(f"  harel run      {path} -e Submit -e Approve")
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    from harel.definition.validate import validate

    defn = _load(args.file, args.name)
    issues = validate(defn)
    for issue in issues:
        print(issue)
    if not issues:
        print(f"{defn.id}: ok")
    return 1 if any(i.severity == "error" for i in issues) else 0


def _cmd_render(args: argparse.Namespace) -> int:
    defn = _load(args.file, args.name)
    if args.mermaid:
        from harel.viz import mermaid

        print(mermaid.render(defn))
    else:
        from harel.viz.plantuml import render

        print(render(defn))
    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    from harel.dsl.parser import parse

    prog = parse(Path(args.file).read_text())
    print("machines:  " + (", ".join(prog.machines) or "—"))
    print("fragments: " + (", ".join(prog.fragments) or "—"))
    print("events:    " + (", ".join(prog.events) or "—"))
    return 0


def _event(spec: str):
    """Parse a `KIND` or `KIND:JSON` event spec into an Event."""
    from harel.spec.states import Event

    kind, sep, raw = spec.partition(":")
    return Event(kind=kind, data=json.loads(raw)) if sep else Event(kind=kind)


def _cmd_run(args: argparse.Namespace) -> int:
    import os

    from harel.engine.durable import DurableRunner
    from harel.engine.store import DictStore

    # resolve the machine's action modules: the working directory (for package-qualified
    # paths like `pkg.mod.fn`, run from the project root) and the .stm file's own directory
    # (for a sibling module). Mirrors `python -m`'s sys.path[0] = cwd.
    sys.path.insert(0, str(Path(args.file).resolve().parent))
    sys.path.insert(0, os.getcwd())
    defn = _load(args.file, args.name)
    if args.validate:
        from harel.definition.validate import validate_or_raise

        validate_or_raise(defn)

    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id, context=json.loads(args.seed) if args.seed else None)
    print(f"(start)              -> {exe.active_path}")
    for spec in args.event or []:
        event = _event(spec)
        exe = runner.process(exe.id, event)
        print(f"{event.kind:<20} -> {exe.active_path}")
    print(f"status: {exe.status.name}  outcome: {exe.outcome}")
    return 0


def _version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("harel")
    except PackageNotFoundError:  # running from a source tree without an install
        return "0+unknown"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="harel", description="Durable, distributed statecharts — tooling CLI."
    )
    parser.add_argument("--version", action="version", version=f"harel {_version()}")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("new", help="scaffold a starter .stm machine (validates + runs out of the box)")
    p.add_argument("file", help="path of the .stm to create")
    p.add_argument("name", nargs="?", help="machine name (default: derived from the file name)")
    p.add_argument("--force", action="store_true", help="overwrite the file if it already exists")
    p.add_argument("--template", choices=["flat"], default="flat", help="starter template (default: flat)")
    p.set_defaults(func=_cmd_new)

    p = sub.add_parser("validate", help="parse and validate a .stm file")
    p.add_argument("file")
    p.add_argument("name", nargs="?", help="machine to select (if the file declares more than one)")
    p.set_defaults(func=_cmd_validate)

    p = sub.add_parser("render", help="render a machine to PlantUML (default) or Mermaid")
    p.add_argument("file")
    p.add_argument("name", nargs="?")
    p.add_argument("--mermaid", action="store_true", help="emit Mermaid stateDiagram-v2 instead")
    p.set_defaults(func=_cmd_render)

    p = sub.add_parser("list", help="list the machines, fragments and events a file declares")
    p.add_argument("file")
    p.set_defaults(func=_cmd_list)

    p = sub.add_parser("run", help="drive a machine with events over an in-memory store")
    p.add_argument("file")
    p.add_argument("name", nargs="?")
    p.add_argument(
        "-e",
        "--event",
        action="append",
        metavar="KIND[:JSON]",
        help="an event to deliver (repeatable); attach data as KIND:'{...}'",
    )
    p.add_argument("--seed", metavar="JSON", help="initial context as a JSON object")
    p.add_argument("--validate", action="store_true", help="validate before running")
    p.set_defaults(func=_cmd_run)

    # `fmt` and `lsp` are passthroughs handled in main() before parsing (so their
    # own flags, e.g. `--check`, reach the underlying tools untouched); declared here
    # only so they show up in `harel -h`.
    sub.add_parser("fmt", help="format .stm files (passthrough to the formatter; `harel fmt -h`)")
    sub.add_parser("lsp", help="start the DSL language server (stdio)")
    sub.add_parser(
        "monitor", help="monitor executions in a TUI (requires the `tui` extra; `harel monitor -h`)"
    )

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    from harel.definition.validate import ValidationError
    from harel.dsl import DslError

    raw = list(sys.argv[1:] if argv is None else argv)

    # passthrough subcommands: hand the remaining args straight to the underlying tool
    if raw and raw[0] == "fmt":
        from harel.fmt import _run

        return _run(raw[1:])
    if raw and raw[0] == "lsp":
        from harel.lsp import main as lsp_main

        lsp_main()
        return 0
    if raw and raw[0] in ("monitor", "tui"):
        from harel.tui import main as tui_main

        return tui_main(raw[1:])

    args = build_parser().parse_args(raw)
    try:
        return args.func(args)
    except (DslError, ValidationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
