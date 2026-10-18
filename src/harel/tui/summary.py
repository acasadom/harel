"""Pure formatting helpers for the monitor (no textual): status glyph/colour, path
shortening, context/value truncation. The UI layer combines these into Rich markup;
keeping them here makes them unit-testable without a terminal."""

from __future__ import annotations

from typing import Optional

from harel.engine.execution import ExecutionSummary, Status

# glyph + colour per lifecycle status. Colours are hex values or names both Rich (the list's
# cells) and Textual (the detail's markup) read alike — Textual takes colour names as CSS ones,
# so `green` is a dark green there — and plain strings here, so this module stays
# dependency-free.
STATUS_STYLE: dict[Status, tuple[str, str]] = {
    Status.PENDING: ("○", "#9e9e9e"),
    Status.RUNNING: ("●", "bold #5fd75f"),
    Status.SUSPENDED: ("⏸", "yellow"),
    Status.CANCELLING: ("◐", "yellow"),
    Status.CANCELLED: ("⊘", "red"),
    Status.DONE: ("✓", "cyan"),
    Status.FAILED: ("✗", "bold red"),
}


def status_glyph(status: Status) -> str:
    return STATUS_STYLE.get(status, ("?", "white"))[0]


def status_color(status: Status) -> str:
    return STATUS_STYLE.get(status, ("?", "white"))[1]


def status_label(status: Status) -> str:
    """`● RUNNING` — the glyph plus the status name (no colour markup)."""
    return f"{status_glyph(status)} {status.value}"


def short_path(active_path: Optional[str], width: int = 40) -> str:
    """Shorten a dotted full_path for a narrow column: keep the tail (the most specific
    states), eliding the head with `…`. `A.B.C.D.E` -> `…C.D.E` when over width."""
    if not active_path:
        return "—"
    if len(active_path) <= width:
        return active_path
    return "…" + active_path[-(width - 1) :]


def truncate(value: object, width: int = 60) -> str:
    """One-line, bounded preview of a context value (never dumps a huge blob)."""
    text = str(value).replace("\n", " ")
    return text if len(text) <= width else text[: width - 1] + "…"


def age(ts: Optional[float], now: float) -> str:
    """How long ago `ts` was, compactly: `12s`, `5m`, `3h`, `2d` (a dash when unknown)."""
    if ts is None:
        return "—"
    secs = max(0.0, now - ts)
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if secs >= size:
            return f"{int(secs // size)}{unit}"
    return f"{int(secs)}s"


def machine(definition_id: str, version: Optional[int]) -> str:
    """`order v2` — a machine and the version an execution runs (v1 when it has none recorded)."""
    return f"{definition_id} v{version or 1}"


def verdict(summary: ExecutionSummary) -> str:
    """The domain verdict cell: the outcome label if any, else a dash."""
    return summary.outcome if summary.outcome else "—"
