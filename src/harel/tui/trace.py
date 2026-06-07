"""The execution-trace model the timeline renders: one `TraceStep` per processed event,
with what went in (event + context) and what came out (transition + actions/guards +
context). The engine does NOT yet persist this — it keeps a state snapshot, not an event
log. For now a preview store seam (`read_trace`/`append_trace` on Sqlite/Dict) holds it,
seeded by the demo; a future engine feature will have the Driver record it for real.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class TraceStep:
    """One step of an execution's history: the event that drove it, the transition it
    took, the actions/guards involved, and the context before and after."""

    index: int
    event_kind: str
    from_path: Optional[str]
    to_path: Optional[str]
    context_in: dict = field(default_factory=dict)
    context_out: dict = field(default_factory=dict)
    actions: tuple[str, ...] = ()
    guards: tuple[str, ...] = ()
    event_data: dict = field(default_factory=dict)
    timestamp: Optional[float] = None
    note: str = ""

    @classmethod
    def from_dict(cls, raw: dict) -> "TraceStep":
        return cls(
            index=raw["index"],
            event_kind=raw.get("event_kind", "—"),
            from_path=raw.get("from_path"),
            to_path=raw.get("to_path"),
            context_in=raw.get("context_in", {}),
            context_out=raw.get("context_out", {}),
            actions=tuple(raw.get("actions", ())),
            guards=tuple(raw.get("guards", ())),
            event_data=raw.get("event_data", {}),
            timestamp=raw.get("timestamp"),
            note=raw.get("note", ""),
        )

    def title(self) -> str:
        """A one-line label for the timeline list: `② Cart → Checkout  on Checkout`."""
        circled = "①②③④⑤⑥⑦⑧⑨⑩"
        marker = circled[self.index] if self.index < len(circled) else f"({self.index + 1})"
        frm = self.from_path.split(".")[-1] if self.from_path else "∅"
        to = self.to_path.split(".")[-1] if self.to_path else "∅"
        return f"{marker} {frm} → {to}  on {self.event_kind}"
