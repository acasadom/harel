"""Render a `Definition` to a Mermaid ``stateDiagram-v2``.

The browser-friendly sibling of `plantuml.render`: it walks the same `Definition`
node tree by references and emits Mermaid, which the VSCode preview webview draws
directly with `mermaid.js` (no Java / render server, unlike PlantUML). Read-only —
this is a live view of the `.stm`, not a graphical editor.

Mermaid constraints that shape the output (verified against mermaid v11):
- a *group* (composite) node may carry only a label, not separate ``id : desc``
  description lines — so a composite's hooks/timeout are folded into its title
  with ``<br/>``; only **leaf** states get description lines;
- concurrency is expressed with ``--`` separators *inside* one composite block, so
  an orthogonal state's regions are emitted as nested composites split by ``--``;
- node ids must be identifier-safe, so `full_path` (which carries spaces/dots) is
  sanitized to an id and the human name is shown via ``state "Name" as id``.
"""

from __future__ import annotations

import re
from typing import Callable, Iterable, Optional, Union

from harel.definition.model import (
    ActionRef,
    Definition,
    EventFilter,
    Node,
    NodeKind,
    Transition,
    is_descendant,
    resolve_relative,
)
from harel.viz._guards import branch_text, effect_text, guard_text

_INDENT = "  "
_HOOKS = (("on_enter", "on enter"), ("on_activity", "on activity"), ("on_exit", "on exit"))


def _short_name(fn: Union[str, Callable]) -> str:
    if callable(fn):
        name = fn.__name__
    elif "." in fn:
        name = fn.rsplit(".", 1)[1]
    else:
        name = fn
    return name.rsplit(".", 1)[-1]


def _nid(node: Node) -> str:
    """An identifier-safe Mermaid id from the node's stable address."""
    return re.sub(r"[^0-9A-Za-z_]", "_", node.full_path)


def _fmt_timeout(timeout: Union[int, dict]) -> str:
    if isinstance(timeout, dict) and "context" in timeout:
        return f"context {timeout['context']}"
    return str(timeout)


def _text(label: str) -> str:
    """`label` safe after a ` : ` separator: a `:` in a state description or an edge label
    is a syntax error in Mermaid 11's `stateDiagram-v2`, so it goes as the entity `#58;`
    (drawn as a colon)."""
    return label.replace(":", "#58;")


def _desc_parts(node: Node) -> list[str]:
    """The descriptive lines for a node: hooks, timeout, outcome, invoke."""
    parts: list[str] = []
    for attr, label in _HOOKS:
        action: Optional[ActionRef] = getattr(node, attr)
        if action is not None:
            parts.append(f"{label}: {_short_name(action.function)}")
    if node.timeout is not None:
        parts.append(f"timeout: {_fmt_timeout(node.timeout)}")
    if node.outcome:
        parts.append(f"outcome: {node.outcome}")
    if node.invoke:
        parts.append(f"invoke: {node.invoke}")
    if node.invoke_each is not None:
        loop_var, coll = node.invoke_each
        parts.append(f"invoke each: {loop_var} in {coll}")
    return parts


# `field__op` suffix -> a readable operator for the diagram label.
def _filter_text(ef: Optional[EventFilter]) -> Optional[str]:
    """A transition label from its event filter (``None`` for an automatic edge). The guard
    goes in square brackets, NOT ``{}`` — curly braces open a composite-state block, so a
    ``{...}`` transition label is a syntax error in ``stateDiagram-v2``."""
    if ef is None:
        return None
    guard = guard_text(ef)
    return f"{ef.kind}<br/>[{guard}]" if guard else ef.kind


def _edge_suffix(ef: Optional[EventFilter], assignments: tuple = ()) -> str:
    """` : Event<br/>[guard]<br/>/ effect` — the UML `event [guard] / effect` label."""
    parts = [_text(p) for p in (_filter_text(ef), effect_text(assignments)) if p]
    if len(parts) == 2:
        return f" : {parts[0]}<br/>/ {parts[1]}"
    if parts and ef is None:
        return f" : / {parts[0]}"
    return f" : {parts[0]}" if parts else ""


def _edge(comp: Node, target: Node, line: str, pad: str, out: list[str], escaping: list[str]) -> None:
    """Emit an edge into `target` from inside `comp`'s block — or, when `target` lies outside
    `comp`, at the top level (`escaping`): Mermaid draws a state inside the block where it is
    first referenced, so an edge out of a composite written inside it would pull its target in.
    State ids are global, so a top-level edge reaches a nested source."""
    if comp.parent is None or (target is not comp and is_descendant(target, comp)):
        out.append(pad + line)
    else:
        escaping.append(line)


def _emit_choice(
    comp: Node, source: Node, t: Transition, pad: str, out: list[str], escaping: list[str]
) -> None:
    choice = t.choice
    assert choice is not None
    src = _nid(source)
    node = f"{src}__choose"
    out.append(f"{pad}state {node} <<choice>>")
    out.append(f"{pad}{src} --> {node}{_edge_suffix(t.event_filter, t.assignments)}")
    for i, (guard, target) in enumerate(choice.branches):
        assigns = choice.branch_assignments[i] if i < len(choice.branch_assignments) else ()
        label = f"[{_text(branch_text(guard))}]" + _branch_effect(assigns)
        _edge(comp, target, f"{node} --> {_nid(target)} : {label}", pad, out, escaping)
    if choice.default is not None:
        label = "else" + _branch_effect(choice.default_assignments)
        _edge(comp, choice.default, f"{node} --> {_nid(choice.default)} : {label}", pad, out, escaping)


def _branch_effect(assignments: tuple) -> str:
    """A `choose` branch's own `set`, appended to its edge label (empty if it has none)."""
    effect = effect_text(assignments)
    return "<br/>/ " + _text(effect) if effect else ""


def _emit_selector(
    comp: Node, source: Node, t: Transition, pad: str, out: list[str], escaping: list[str]
) -> None:
    selector = t.selector
    assert selector is not None
    fn = _short_name(selector.action.function)
    src = _nid(source)
    choice = f"{src}__{fn}"
    out.append(f"{pad}state {choice} <<choice>>")
    out.append(f"{pad}{src} --> {choice}{_edge_suffix(t.event_filter)}")
    for value, target_name in selector.mapper.items():
        target = resolve_relative(comp, target_name)
        assert target is not None, f"selector target {target_name!r} unresolved in {comp.full_path!r}"
        _edge(comp, target, f"{choice} --> {_nid(target)} : {fn}={_text(str(value))}", pad, out, escaping)
    if selector.default is not None:
        target = resolve_relative(comp, selector.default)
        assert target is not None, f"selector else {selector.default!r} unresolved in {comp.full_path!r}"
        _edge(comp, target, f"{choice} --> {_nid(target)} : else", pad, out, escaping)


def _emit_transitions(comp: Node, pad: str, out: list[str], escaping: list[str]) -> None:
    for child in comp.children:
        for t in (t for t in comp.transitions if t.source is child):
            if t.target is not None:
                line = f"{_nid(child)} --> {_nid(t.target)}{_edge_suffix(t.event_filter, t.assignments)}"
                _edge(comp, t.target, line, pad, out, escaping)
            elif t.selector is not None:
                _emit_selector(comp, child, t, pad, out, escaping)
            elif t.choice is not None:
                _emit_choice(comp, child, t, pad, out, escaping)


def _emit_leaf(node: Node, pad: str, out: list[str]) -> None:
    nid = _nid(node)
    parts = _desc_parts(node)
    # declared with its name whenever it has a description: Mermaid draws a described state's
    # description alone, without its name, unless the state is declared `state "Name" as id`
    if node.name != nid or parts:
        out.append(f'{pad}state "{node.name}" as {nid}')
    for part in parts:
        out.append(f"{pad}{nid} : {_text(part)}")


def _composite_title(node: Node) -> str:
    """A composite carries its hooks/timeout folded into the label (a group node
    cannot have separate description lines in Mermaid)."""
    parts = _desc_parts(node)
    return node.name + "<br/>" + "<br/>".join(parts) if parts else node.name


def _emit_composite(node: Node, indent: int, out: list[str], escaping: list[str]) -> None:
    pad = _INDENT * indent
    nid = _nid(node)
    title = _composite_title(node)
    head = f"{pad}state {nid} {{" if title == nid else f'{pad}state "{title}" as {nid} {{'
    out.append(head)
    if node.kind is NodeKind.ORTHOGONAL:
        for i, region in enumerate(node.children):
            if i:
                out.append(f"{_INDENT * (indent + 1)}--")
            _emit_composite(region, indent + 1, out, escaping)
    else:
        _emit_body(node, indent + 1, out, escaping)
    out.append(f"{pad}}}")


def _emit_body(comp: Node, indent: int, out: list[str], escaping: list[str]) -> None:
    pad = _INDENT * indent
    if comp.start_state is not None:
        start = comp.child(comp.start_state)
        assert start is not None, f"start_state {comp.start_state!r} not a child of {comp.full_path!r}"
        out.append(f"{pad}[*] --> {_nid(start)}")
    for child in comp.children:
        if child.is_composite:
            _emit_composite(child, indent, out, escaping)
        else:
            _emit_leaf(child, pad, out)
    _emit_transitions(comp, pad, out, escaping)
    # a leaf with no outgoing transition in this scope is a sink → final pseudostate
    for child in comp.children:
        if not child.is_composite and not any(t.source is child for t in comp.transitions):
            line = f"{pad}{_nid(child)} --> [*]"
            if line not in out:
                out.append(line)


ACTIVE_STYLE = "fill:#fde68a,stroke:#b45309,stroke-width:2px"


def node_id(definition: Definition, path: str) -> str:
    """The Mermaid id `render` gives the state at `path` (its full path, e.g. `Shipping.Packing`)
    — for a `class` or `style` line of your own. Raises `ValueError` for a path the machine
    doesn't have."""
    node = definition.index.get(path) if path else None
    if node is None:
        raise ValueError(f"no state {path!r} in machine {definition.id!r}")
    return _nid(node)


def render(definition: Definition, *, active: Iterable[str] = (), active_style: str = ACTIVE_STYLE) -> str:
    """`definition` as a Mermaid `stateDiagram-v2`. `active` lists the states to highlight, by
    full path — an execution's `active_path`, its regions' — styled with `active_style` (a Mermaid
    `classDef` body). A path the machine doesn't have raises `ValueError`."""
    out: list[str] = ["stateDiagram-v2"]
    escaping: list[str] = []  # edges out of a composite, written at the top level
    _emit_body(definition.root, 0, out, escaping)
    out.extend(escaping)
    ids = list(dict.fromkeys(node_id(definition, path) for path in active))
    if ids:
        out.append(f"classDef active {active_style}")
        out.append(f"class {','.join(ids)} active")
    return "\n".join(out)
