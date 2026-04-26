"""Run every runnable code block in the docs so the examples never drift from the engine.

Each ```python fenced block under ``docs/**/*.md`` is executed; blocks accumulate in a
per-file namespace, so a tutorial page reads as one continuous session (later blocks see
the imports and variables of earlier ones). The first block on a page is therefore
self-contained (it imports what it needs); the rest continue from it.

Mark a block as non-executable (illustrative pseudo-code, a partial signature, a snippet
that intentionally raises) by making ``# docs-test: skip`` its first line.

DSL listings use ```text and console output uses ```text too, so only real, runnable
Python is executed here. The test only asserts a block runs without raising — exact
printed output is shown in the docs but not asserted (it would be brittle)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("could not locate the repo root (no pyproject.toml found)")


DOCS = _repo_root() / "docs"

# A ```python … ``` fence at the start of a line (DOTALL so it spans lines, non-greedy).
_PY_FENCE = re.compile(r"^```python\n(.*?)^```", re.MULTILINE | re.DOTALL)


def _doc_files() -> list[Path]:
    if not DOCS.exists():
        return []
    return sorted(p for p in DOCS.rglob("*.md") if "_build" not in p.parts)


@pytest.mark.parametrize("doc", _doc_files(), ids=lambda p: str(p.relative_to(_repo_root())))
def test_doc_examples_run(doc: Path) -> None:
    blocks = [
        code
        for code in _PY_FENCE.findall(doc.read_text())
        if not code.lstrip().startswith("# docs-test: skip")
    ]
    if not blocks:
        pytest.skip("no runnable python blocks")

    namespace: dict = {}
    for i, code in enumerate(blocks):
        try:
            exec(compile(code, f"{doc}#block{i}", "exec"), namespace)
        except Exception as exc:  # noqa: BLE001 — report which block/file failed
            pytest.fail(
                f"{doc.name} block {i} raised {type(exc).__name__}: {exc}\n-------- block --------\n{code}"
            )
