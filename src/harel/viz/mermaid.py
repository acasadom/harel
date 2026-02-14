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
from typing import Callable, Optional, Union

from harel.definition.model import (
    ActionRef,
    Definition,
    EventFilter,
    Node,
    NodeKind,
    Transition,
    resolve_relative,
)

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


def _filter_text(ef: Optional[EventFilter]) -> Optional[str]:
    """A transition label from its event filter (``None`` for an automatic edge)."""
    if ef is None:
        return None
    if ef.predicates:
        data = str(ef.predicates).replace("'", "").replace(": ", "=")
        return f"{ef.kind}<br/>{data}"
    return ef.kind


def _edge_suffix(ef: Optional[EventFilter]) -> str:
    label = _filter_text(ef)
    return f" : {label}" if label else ""


def _emit_selector(comp: Node, source: Node, t: Transition, pad: str, out: list[str]) -> None:
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
        out.append(f"{pad}{choice} --> {_nid(target)} : {fn}={value}")
    if selector.default is not None:
        target = resolve_relative(comp, selector.default)
        assert target is not None, f"selector else {selector.default!r} unresolved in {comp.full_path!r}"
        out.append(f"{pad}{choice} --> {_nid(target)} : else")


def _emit_transitions(comp: Node, pad: str, out: list[str]) -> None:
    for child in comp.children:
        for t in (t for t in comp.transitions if t.source is child):
            if t.target is not None:
                out.append(f"{pad}{_nid(child)} --> {_nid(t.target)}{_edge_suffix(t.event_filter)}")
            elif t.selector is not None:
                _emit_selector(comp, child, t, pad, out)


def _emit_leaf(node: Node, pad: str, out: list[str]) -> None:
    nid = _nid(node)
    if node.name != nid:
        out.append(f'{pad}state "{node.name}" as {nid}')
    for part in _desc_parts(node):
        out.append(f"{pad}{nid} : {part}")


def _composite_title(node: Node) -> str:
    """A composite carries its hooks/timeout folded into the label (a group node
    cannot have separate description lines in Mermaid)."""
    parts = _desc_parts(node)
    return node.name + "<br/>" + "<br/>".join(parts) if parts else node.name


def _emit_composite(node: Node, indent: int, out: list[str]) -> None:
    pad = _INDENT * indent
    nid = _nid(node)
    title = _composite_title(node)
    head = f"{pad}state {nid} {{" if title == nid else f'{pad}state "{title}" as {nid} {{'
    out.append(head)
    if node.kind is NodeKind.ORTHOGONAL:
        for i, region in enumerate(node.children):
            if i:
                out.append(f"{_INDENT * (indent + 1)}--")
            _emit_composite(region, indent + 1, out)
    else:
        _emit_body(node, indent + 1, out)
    out.append(f"{pad}}}")


def _emit_body(comp: Node, indent: int, out: list[str]) -> None:
    pad = _INDENT * indent
    if comp.start_state is not None:
        start = comp.child(comp.start_state)
        assert start is not None, f"start_state {comp.start_state!r} not a child of {comp.full_path!r}"
        out.append(f"{pad}[*] --> {_nid(start)}")
    for child in comp.children:
        if child.is_composite:
            _emit_composite(child, indent, out)
        else:
            _emit_leaf(child, pad, out)
    _emit_transitions(comp, pad, out)
    # a leaf with no outgoing transition in this scope is a sink → final pseudostate
    for child in comp.children:
        if not child.is_composite and not any(t.source is child for t in comp.transitions):
            line = f"{pad}{_nid(child)} --> [*]"
            if line not in out:
                out.append(line)


def render(definition: Definition) -> str:
    out: list[str] = ["stateDiagram-v2"]
    _emit_body(definition.root, 0, out)
    return "\n".join(out)
