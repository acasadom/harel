"""Render a `Definition` to PlantUML.

The marshmallow/full_path-free replacement for `stm_to_plantuml`: it walks the
`Definition` node tree by references instead of parsing path strings, so it has
no `_get_sibling_state` / prefix juggling. It reproduces the exact text the old
generator emits (the parity tests compare the line sets).
"""

from __future__ import annotations

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
_HOOKS = ("on_enter", "on_activity", "on_exit")
_ORTHOGONAL = (NodeKind.ORTHOGONAL,)


def _short_name(fn: Union[str, Callable]) -> str:
    if callable(fn):
        name = fn.__name__
    elif "." in fn:
        name = fn.rsplit(".", 1)[1]
    else:
        name = fn
    return name.rsplit(".", 1)[-1]


def _prefixed(node: Node) -> str:
    return node.full_path.replace(" ", "")


def _filter(ef: Optional[EventFilter]) -> str:
    if ef is None:
        return ""
    if ef.predicates:
        data = str(ef.predicates).replace("'", "").replace(": ", "=")
        return f": {ef.kind}\\n {data}"
    return f": {ef.kind}"


def _emit_selector(comp: Node, source: Node, t: Transition, pad: str, out: list[str]) -> None:
    selector = t.selector
    assert selector is not None
    fn = _short_name(selector.action.function)
    src = _prefixed(source)
    choice = ".".join([*src.split(".")[:-1], fn])
    out.append(f"{pad}state {choice}<<choice>>")
    out.append(f"{pad}{src} --> {choice}{_filter(t.event_filter)}")
    for value, target_name in selector.mapper.items():
        target = resolve_relative(comp, target_name)
        assert target is not None, f"selector target {target_name!r} unresolved in {comp.full_path!r}"
        out.append(f"{pad}{choice} --> {_prefixed(target)}: {fn}={value}")


def _emit_transitions(comp: Node, pad: str, out: list[str]) -> None:
    for child in comp.children:
        trans = [t for t in comp.transitions if t.source is child]
        if not trans:
            line = f"{pad}{_prefixed(child)} --> [*]"
            if line not in out:
                out.append(line)
            continue
        for t in trans:
            if t.target is not None:
                out.append(f"{pad}{_prefixed(child)} --> {_prefixed(t.target)}{_filter(t.event_filter)}")
            elif t.selector is not None:
                _emit_selector(comp, child, t, pad, out)


def _emit(comp: Node, indent: int, end_line: str) -> list[str]:
    pad = _INDENT * indent
    out: list[str] = []

    # 1. action declarations for each child
    for child in comp.children:
        pref = _prefixed(child)
        for hook in _HOOKS:
            action: Optional[ActionRef] = getattr(child, hook)
            if action is None:
                continue
            fn = _short_name(action.function)
            if child.name == pref:
                out.append(f"{pad}state {child.name}: <b>{hook}</b>: <i>{fn}</i>")
            else:
                out.append(f'{pad}state "{child.name}" as {pref}: <b>{hook}</b>: <i>{fn}</i>')

    # 2. composite child blocks (recurse)
    for child in comp.children:
        if child.is_composite:
            child_end = "||" if child.kind in _ORTHOGONAL else ""
            sub = _emit(child, indent + 1, child_end)
            pref = _prefixed(child)
            if child.name == pref:
                out.append(f"{pad}state {child.name} {{")
            else:
                out.append(f'{pad}state "{child.name}" as {pref} {{')
            out.extend(sub)
            out.append(f"{pad}}}")
            out.append(f"{pad}{end_line}")

    # 3. initial transition
    if comp.start_state is not None:
        start = comp.child(comp.start_state)
        assert start is not None, f"start_state {comp.start_state!r} not a child of {comp.full_path!r}"
        out.append(f"{pad}[*] --> {_prefixed(start)}")

    # 4. transitions / sinks (or drop the trailing region separator)
    if comp.transitions:
        _emit_transitions(comp, pad, out)
    elif out:
        out.pop()  # remove the last appended end_line ("||" between orthogonal regions)

    return out


def render(definition: Definition) -> str:
    return "\n".join(_emit(definition.root, 0, ""))
