"""The example pages draw each machine as its `.stm` renders now: a `<!-- machine: <stm> <name> -->`
line before a ```{mermaid} block marks the block as `harel.viz.mermaid.render` of that machine.

When a machine changes, regenerate the blocks:

    uv run python test/unit/docs/test_example_diagrams.py --write
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
PAGES = sorted((ROOT / "docs" / "examples").glob("*.md"))
_MARKED = re.compile(r"(<!-- machine: (\S+) (\S+) -->\n```\{mermaid\}\n)(.*?)(```)", re.S)


def _render(stm: str, name: str) -> str:
    from harel import definition_from_dsl_file
    from harel.viz import mermaid

    return mermaid.render(definition_from_dsl_file(ROOT / stm, name)).rstrip() + "\n"


def _regenerated(text: str) -> str:
    return _MARKED.sub(lambda m: m.group(1) + _render(m.group(2), m.group(3)) + m.group(5), text)


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_each_machine_is_drawn_as_it_renders(page: Path) -> None:
    text = page.read_text()
    assert text == _regenerated(text), (
        f"{page.name}: a machine diagram is out of date — "
        "run `uv run python test/unit/docs/test_example_diagrams.py --write`"
    )


def test_every_example_page_draws_its_machine() -> None:
    assert PAGES and all(_MARKED.search(p.read_text()) for p in PAGES if p.name != "index.md")


if __name__ == "__main__" and "--write" in sys.argv:
    for page in PAGES:
        page.write_text(_regenerated(page.read_text()))
        print("wrote", page.relative_to(ROOT))
