"""Rendering helpers for the monitor UI: populate a Textual `Tree` from the pure
`TreeModel`, and build the markup for the data panels. Kept thin — all the logic lives
in the pure layer (`tree`/`summary`); this only turns it into Rich markup / tree nodes."""

from __future__ import annotations

from rich.markup import escape
from rich.text import Text
from textual.widgets import Tree

from harel.definition.model import NodeKind
from harel.tui import summary
from harel.tui.model import ExecutionDetail
from harel.tui.trace import TraceStep
from harel.tui.tree import NodeMark, TreeModel, TreeNode

_KIND_GLYPH = {
    NodeKind.LEAF: "·",
    NodeKind.COMPOSITE: "▸",
    NodeKind.PARALLEL: "⊞",
    NodeKind.ORTHOGONAL: "⊞",
}


def node_label(tn: TreeNode, *, selected: bool = False) -> Text:
    """A Rich `Text` label for one statechart node: kind glyph + name (reverse-green if
    the active leaf, bold if on the active path) + an optional region annotation. When
    `selected` (the timeline's currently-navigated step lands here) it's underlined with a
    `◀` marker, so you can see both the live state (colour) and the navigated step."""
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
        state = f"✓ {r.outcome}" if r.finished else "…"
        label += Text(f"  ({'invoke' if r.submachine else 'region'}: {state})", style="dim")
    if selected:
        label += Text("  ◀", style="bold yellow")
        label.stylize("underline yellow")
    return label


def populate_statechart(tree: Tree, model: TreeModel, selected_path: str | None = None) -> None:
    """Fill a Textual `Tree` from a `TreeModel`, marking `selected_path` (the navigated
    timeline step). Each node's `data` is its full path (what a redrive targets); a leaf state
    is a tree leaf, with nothing to expand. Statecharts are shallow, so re-populating on each
    navigation is cheap. With no resolved Definition, show a single data-only placeholder."""
    tree.clear()
    if not model.resolved or model.root is None:
        tree.root.set_label(Text("(definition unavailable — data-only)", style="dim italic"))
        tree.root.data = None
        return
    tree.root.set_label(node_label(model.root, selected=model.root.full_path == selected_path))
    tree.root.data = model.root.full_path

    def add(parent, tn: TreeNode) -> None:
        for child in tn.children:
            label = node_label(child, selected=child.full_path == selected_path)
            if child.children:
                add(parent.add(label, data=child.full_path, expand=True), child)
            else:
                parent.add_leaf(label, data=child.full_path)

    add(tree.root, model.root)
    tree.root.expand()


def find_node(tree: Tree, path: str):
    """The node of `tree` whose data is `path` (a state's full path), or None."""
    stack = [tree.root]
    while stack:
        node = stack.pop()
        if node.data == path:
            return node
        stack.extend(node.children)
    return None


def step_markup(step: TraceStep) -> str:
    """The detail of one navigated timeline step: event in, transition, actions/guards,
    and the context before → after."""

    def kv(d: dict) -> str:
        return (
            "  " + "\n  ".join(f"[cyan]{k}[/] = {summary.truncate(v, 60)}" for k, v in d.items())
            if d
            else "  [dim](empty)[/]"
        )

    frm = step.from_path or "∅"
    to = step.to_path or "∅"
    parts = [
        f"[b]event[/]       [yellow]{step.event_kind}[/]"
        + (f"  {summary.truncate(step.event_data, 50)}" if step.event_data else ""),
        f"[b]transition[/]  {frm} → [green]{to}[/]",
    ]
    if step.guards:
        parts.append("[b]guards[/]      " + ", ".join(step.guards))
    if step.actions:
        parts.append("[b]actions[/]     " + ", ".join(step.actions))
    if step.assigned:
        parts.append(
            "[b]set[/]         "
            + ", ".join(f"context.{k} = {summary.truncate(v, 30)}" for k, v in step.assigned.items())
        )
    parts.append("[b u]context in[/]\n" + kv(step.context_in))
    parts.append("[b u]context out[/]\n" + kv(step.context_out))
    return "\n".join(parts)


def status_header_markup(detail: ExecutionDetail, now: float) -> str:
    """The detail screen's header: status, id, the machine version and its source digest, the
    routing key when it isn't the machine's own (a child of another machine's tree), the
    parent, the outcome, and when it was created and last written."""
    exe = detail.execution
    color = summary.status_color(exe.status)
    lines = [
        f"[{color}]{summary.status_label(exe.status)}[/]   [b]{exe.id}[/]",
        "machine  [b]"
        + summary.machine(exe.definition_id, exe.definition_version)
        + "[/]"
        + (f"  [dim]digest {exe.definition_digest}[/]" if exe.definition_digest else ""),
    ]
    tree_bits = []
    if exe.routing_key and exe.routing_key != exe.definition_id:
        tree_bits.append(f"tree [b]{exe.routing_key}[/]")
    if exe.parent_id:
        tree_bits.append(f"parent [b]{exe.parent_id}[/] [dim](u)[/]")
    if tree_bits:
        lines.append("   ".join(tree_bits))
    bits = []
    if exe.outcome:
        bits.append(f"outcome [b]{exe.outcome}[/]")
    bits.append(
        f"created {summary.age(exe.created_at, now)} ago · written {summary.age(exe.updated_at, now)} ago"
    )
    lines.append("   ".join(bits))
    return "\n".join(lines)


def error_markup(detail: ExecutionDetail) -> str:
    """The execution's error, whole (a dead letter's reason, a `DefinitionChanged`, …)."""
    return f"[b red]error[/]\n{escape(detail.execution.error or '')}"


def context_markup(detail: ExecutionDetail) -> str:
    """The execution context as bounded key/value lines (never a giant blob)."""
    ctx = detail.execution.context
    if not ctx:
        return "[dim](empty context)[/]"
    return "\n".join(
        f"[cyan]{escape(str(k))}[/] = {escape(summary.truncate(v, width=70))}" for k, v in ctx.items()
    )


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
    return "\n".join(f"[magenta]{escape(k or '(machine)')}[/] → {escape(v)}" for k, v in hist.items())
