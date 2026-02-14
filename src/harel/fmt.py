"""A canonical formatter for the statechart DSL (`.stm`) — the `gofmt` of the DSL.

`format_text(src)` reindents by brace/bracket depth (2 spaces per level), trims
trailing whitespace, collapses runs of blank lines to one, and drops leading /
trailing blank lines. It preserves **everything else verbatim** — comments, the
author's line structure, and the exact content of each line — so it is safe and
idempotent (`format_text(format_text(x)) == format_text(x)`).

It is deliberately a *reindenter*, not a reflow: a block written on one line
(`state A { on enter f }`) is kept on one line. Layout-significant whitespace does
not exist in the grammar (strings are single-line; only leading indentation and
trailing space are touched), so formatting never changes the parsed program.

CLI: `harel-fmt FILE...` rewrites in place; `--check` reports (exit 2) without
writing; `--diff` prints a unified diff. Also `python -m harel.fmt`.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

INDENT = "  "  # two spaces per level, matching the project style
_OPEN, _CLOSE = "{[(", "}])"


def _scan(line: str) -> tuple[int, int, int]:
    """Scan one stripped line for bracket structure, ignoring brackets inside
    strings and after a line comment (`#` / `//`). Returns (opens, closes,
    leading_closes) — totals drive the running depth; leading_closes is the run of
    closing brackets at the very start (so a line beginning with `}` dedents itself)."""
    opens = closes = leading = 0
    seen_other = False
    in_str = False
    i, n = 0, len(line)
    while i < n:
        c = line[i]
        if in_str:
            if c == "\\":
                i += 2
                continue
            if c == '"':
                in_str = False
            i += 1
            continue
        if c == '"':
            in_str = True
            seen_other = True
        elif c == "#" or (c == "/" and i + 1 < n and line[i + 1] == "/"):
            break  # rest of the line is a comment
        elif c in _OPEN:
            opens += 1
            seen_other = True
        elif c in _CLOSE:
            closes += 1
            if not seen_other:
                leading += 1
        elif not c.isspace():
            seen_other = True
        i += 1
    return opens, closes, leading


def format_text(src: str) -> str:
    """Return the canonically-formatted form of `src` (see the module docstring)."""
    out: list[str] = []
    level = 0
    pending_blank = False
    for raw in src.splitlines():
        stripped = raw.strip()
        if not stripped:
            pending_blank = bool(out)  # collapse runs; suppress leading blanks
            continue
        if pending_blank:
            out.append("")
            pending_blank = False
        opens, closes, leading = _scan(stripped)
        indent = max(0, level - leading)
        out.append(INDENT * indent + stripped)
        level = max(0, level + opens - closes)
    if not out:
        return ""
    return "\n".join(out) + "\n"


def _run(argv: Optional[list[str]] = None) -> int:
    import argparse
    import difflib

    ap = argparse.ArgumentParser(prog="harel-fmt", description="Format statechart DSL (.stm) files.")
    ap.add_argument("files", nargs="+", type=Path, help="the .stm files to format")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="do not write; exit 2 if any file would change")
    mode.add_argument("--diff", action="store_true", help="print a unified diff instead of writing")
    args = ap.parse_args(argv)

    changed: list[Path] = []
    for path in args.files:
        src = path.read_text()
        out = format_text(src)
        if out == src:
            continue
        changed.append(path)
        if args.diff:
            sys.stdout.writelines(
                difflib.unified_diff(
                    src.splitlines(keepends=True),
                    out.splitlines(keepends=True),
                    fromfile=f"{path} (original)",
                    tofile=f"{path} (formatted)",
                )
            )
        elif not args.check:
            path.write_text(out)
            print(f"formatted {path}")

    if args.check and changed:
        print("would reformat: " + ", ".join(str(p) for p in changed), file=sys.stderr)
        return 2
    return 0


def main() -> None:
    """Console entry point (`harel-fmt`)."""
    sys.exit(_run())


if __name__ == "__main__":
    main()
