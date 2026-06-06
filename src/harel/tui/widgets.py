"""Rendering helpers for the monitor UI: populate a Textual `Tree` from the pure
`TreeModel`, and build the markup for the data panels. Kept thin — all the logic lives
in the pure layer (`tree`/`summary`); this only turns it into Rich markup / tree nodes."""

from __future__ import annotations

from rich.text import Text
from textual.widgets import Tree

from harel.definition.model import NodeKind
from harel.tui import summary
from harel.tui.model import ExecutionDetail
from harel.tui.tree import NodeMark, TreeModel, TreeNode

_KIND_GLYPH = {
    NodeKind.LEAF: "·",
    NodeKind.COMPOSITE: "▸",
    NodeKind.PARALLEL: "⊞",
    NodeKind.ORTHOGONAL: "⊞",
}


def node_label(tn: TreeNode) -> Text:
    """A Rich `Text` label for one statechart node: kind glyph + name (reverse-green if
    active, bold if on the active path) + an optional region annotation."""
    name = tn.name or "(root)"
    if tn.mark is NodeMark.ACTIVE:
        styled = Text(name, style="reverse bold green")
    elif tn.mark is NodeMark.ON_ACTIVE_PATH:
        styled = Text(name, style="bold")
    else:
        styled = Text(name)
    label = Text(f"{_KIND_GLYPH.get(tn.kind, '·')} ") + styled
    if tn.region is not None:
        r = tn.region
        tag = "invoke" if r.submachine else "region"
        state = f"✓ {r.outcome}" if r.finished else ("✓" if r.finished else "…")
        label += Text(f"  ({tag}: {state})", style="dim")
    return label


def populate_statechart(tree: Tree, model: TreeModel) -> None:
    """Fill a Textual `Tree` from a `TreeModel`, expanding the whole tree (statecharts are
    shallow). With no resolved Definition, show a single data-only placeholder."""
    tree.clear()
    if not model.resolved or model.root is None:
        tree.root.set_label(Text("(definition unavailable — data-only)", style="dim italic"))
        tree.root.data = None
        return
    tree.root.set_label(node_label(model.root))
    tree.root.data = model.root.full_path

    def add(parent, tn: TreeNode) -> None:
        for child in tn.children:
            node = parent.add(node_label(child), data=child.full_path)
            add(node, child)

    add(tree.root, model.root)
    tree.root.expand_all()


def status_markup(detail: ExecutionDetail) -> str:
    """The status panel: lifecycle status (coloured), domain outcome, error, ids."""
    exe = detail.execution
    color = summary.status_color(exe.status)
    lines = [
        f"[b]status[/]    [{color}]{summary.status_label(exe.status)}[/]",
        f"[b]outcome[/]   {exe.outcome or '—'}",
        f"[b]active[/]    {summary.short_path(exe.active_path, width=60)}",
        f"[b]version[/]   {exe.version}",
        f"[b]id[/]        {exe.id}",
    ]
    if exe.definition_fqn:
        lines.append(f"[b]invoke[/]    {exe.definition_fqn}")
    if exe.parent_id:
        lines.append(f"[b]parent[/]    {exe.parent_id}")
    if exe.error:
        lines.append(f"[b red]error[/]     {summary.truncate(exe.error, width=80)}")
    return "\n".join(lines)


def context_markup(detail: ExecutionDetail) -> str:
    """The execution context as bounded key/value lines (never a giant blob)."""
    ctx = detail.execution.context
    if not ctx:
        return "[dim](empty context)[/]"
    return "\n".join(f"[cyan]{k}[/] = {summary.truncate(v, width=70)}" for k, v in ctx.items())


def pending_markup(detail: ExecutionDetail, now: float) -> str:
    """Pending work for this execution: durable timers (with time-to-fire), inbound
    queued events, and pending child spawns."""
    out: list[str] = []
    if detail.timers:
        out.append("[b]timers[/]")
        for path, fire_at in detail.timers:
            dt = fire_at - now
            when = f"in {dt:.0f}s" if dt >= 0 else f"{-dt:.0f}s ago"
            out.append(f"  {summary.short_path(path, width=40)}  [dim]({when})[/]")
    if detail.inbound:
        out.append("[b]inbound events[/]")
        out += [f"  {e.event.kind}" for e in detail.inbound]
    if detail.spawns:
        out.append("[b]pending spawns[/]")
        out += [f"  {s.child_id}" for s in detail.spawns]
    return "\n".join(out) if out else "[dim](no pending work)[/]"


def history_markup(detail: ExecutionDetail) -> str:
    """The execution's state-memory: composite -> last active child."""
    hist = detail.execution.history
    if not hist:
        return "[dim](no history)[/]"
    return "\n".join(f"[magenta]{k}[/] → {v}" for k, v in hist.items())
