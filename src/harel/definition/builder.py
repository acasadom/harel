"""Build a `Definition` (node tree with references) from a parsed YAML/JSON dict.

Reuses the structural normalization in `normalize` (extends / transitions /
states). Builds the tree in one recursive pass and resolves transition
`source`/`target` to **node references** in a second pass (all nodes exist by
then). Reference resolution replaces the old runtime path/string navigation:

- `source` is resolved by descending from the transition's scope composite,
- `target` is resolved by sibling-lookup (descend from the scope, then walk up
  ancestors) — the reference-based equivalent of the old `_get_sibling_state`.

This is the marshmallow-free, full_path-free construction path for the new
engine. The legacy `build.py` (which produces the old pydantic objects) stays
until the swap.
"""

from __future__ import annotations

from typing import Any, Optional

from harel.definition.events import EventType, FieldSpec
from harel.definition.model import (
    ActionRef,
    Definition,
    EventFilter,
    Node,
    NodeKind,
    Predicate,
    Selector,
    Transition,
    descend,
    resolve_relative,
)
from harel.definition.normalize import normalize_states, normalize_transitions


class BuildError(Exception):
    """A construction error. `pos` is the (line, column) of the offending DSL node
    when known (the DSL loader stashes it as `__pos__`), so the loader can re-raise
    a located error; it is None on the raw-config path."""

    def __init__(self, message: str, pos: Optional[tuple] = None) -> None:
        super().__init__(message)
        self.pos = pos


# --- small helpers (mirror the relevant bits of build.py) ---------------------


def _resolve_function_path(data: dict, global_context: Optional[dict]) -> dict:
    data = dict(data)
    if global_context:
        module_path = global_context.get("module_path", None)
        stm_sub_path = global_context.get("stm_sub_path", None)
        function = data["function"]
        dots = function[:2].count(".")
        if dots:
            assert module_path is not None
            data["package"] = module_path
        if dots == 1:
            assert stm_sub_path is not None
            data["function"] = f"{stm_sub_path}{function}"
    return data


def _build_action(data: Any, global_context: Optional[dict]) -> Optional[ActionRef]:
    if data is None:
        return None
    if isinstance(data, str):
        data = {"function": data}
    resolved = _resolve_function_path(data, global_context)
    return ActionRef(
        function=resolved["function"],
        inputs=resolved.get("inputs", {}) or {},
        package=resolved.get("package"),
    )


_COMBINATORS = {"all", "any", "not"}


def _parse_predicate(data: dict) -> Optional[Predicate]:
    """Parse a predicate node: combinator keys (`all`/`any` -> list of nodes,
    `not` -> a single node) and/or flat `field__op -> value` leaves. Multiple
    keys at one level are AND-ed."""
    nodes: list[Predicate] = []
    for key, val in data.items():
        if key in ("all", "any"):
            children = [p for d in val if (p := _parse_predicate(d)) is not None]
            nodes.append(Predicate(node=key, children=children))
        elif key == "not":
            child = _parse_predicate(val)
            nodes.append(Predicate(node="not", children=[child] if child is not None else []))
        else:
            name, op = key.split("__") if "__" in key else (key, "eq")
            nodes.append(Predicate(node="leaf", field=name, op=op, value=val))
    if not nodes:
        return None
    return nodes[0] if len(nodes) == 1 else Predicate(node="all", children=nodes)


def _event_filter(kind: str, data: dict) -> EventFilter:
    """Build an EventFilter from a `kind` and a data dict that may mix flat
    `field__op` leaves (kept in `predicates` for back-compat/PlantUML) with the
    composable combinators `all`/`any`/`not` (parsed into `predicate`)."""
    flat = {k: v for k, v in data.items() if k not in _COMBINATORS}
    composite = {k: v for k, v in data.items() if k in _COMBINATORS}
    return EventFilter(
        kind=kind, predicates=flat, predicate=_parse_predicate(composite) if composite else None
    )


def _build_event_filter(data: dict) -> EventFilter:
    return _event_filter(data["type"], {k: v for k, v in data.items() if k != "type"})


def _build_field_spec(data: Any) -> FieldSpec:
    """A field spec is either a bare type string (`status: string`) or a dict
    (`status: {type: string, required: false}`)."""
    if isinstance(data, str):
        return FieldSpec(type=data)
    return FieldSpec(type=data.get("type", "any"), required=data.get("required", True))


def _build_events(config: dict) -> dict[str, EventType]:
    """Parse the optional top-level `events:` block: name -> {field: spec}."""
    raw = config.get("events") or {}
    return {
        name: EventType(name=name, fields={f: _build_field_spec(s) for f, s in (fields or {}).items()})
        for name, fields in raw.items()
    }


def _build_selector(data: dict, global_context: Optional[dict]) -> Selector:
    data = dict(data)
    mapper = {str(k): v for k, v in data.pop("mapper").items()}
    default = data.pop("default", None)
    enum = data.pop("enum", None)
    action = _build_action(data, global_context)
    assert action is not None
    return Selector(action=action, mapper=mapper, default=default, enum=enum)


# --- tree construction --------------------------------------------------------


def _build_node(
    norm: dict,
    parent: Optional[Node],
    global_context: Optional[dict],
    index: dict,
    pending: list,
    submachines: dict,
    mach_name: str,
) -> Node:
    norm = dict(norm)
    name = norm.get("name", "")
    full_path = norm.get("full_path", "")
    type_ = norm.get("type") or ("CompositeState" if "states" in norm else "State")

    node = Node(
        name=name,
        full_path=full_path,
        kind=NodeKind(type_),
        parent=parent,
        on_enter=_build_action(norm.get("on_enter"), global_context),
        on_activity=_build_action(norm.get("on_activity"), global_context),
        on_exit=_build_action(norm.get("on_exit"), global_context),
        timeout=norm.get("timeout"),
        outcome=norm.get("outcome"),
        carry=tuple(norm.get("carry", ())),
        defer=frozenset(norm.get("defer", ())),
        invoke=norm.get("invoke"),
        invoke_with=dict(norm.get("invoke_with", {})),
        invoke_each=tuple(norm["invoke_each"]) if norm.get("invoke_each") else None,
        start_state=norm.get("start"),
        allow_history=norm.get("allow_history", True),
    )
    index[full_path] = node

    if "invoke_inline" in norm:
        # an inline `invoke` target: build it as its OWN Definition with a synthetic
        # FQN (= its id, so the runner resolves it by id), and point `invoke` at it
        syn = f"{mach_name}#{full_path}"
        sub_defn = build_definition(norm["invoke_inline"], global_context, syn, definition_id=syn)
        node.invoke = syn
        submachines[syn] = sub_defn
        submachines.update(sub_defn.submachines)  # flatten nested inline machines

    if "states" in norm:
        sub = normalize_states(normalize_transitions(dict(norm)))
        for child_norm in sub["states"].values():
            node.children.append(
                _build_node(child_norm, node, global_context, index, pending, submachines, mach_name)
            )
        pending.append((node, sub.get("transitions", [])))

    return node


def _build_transition(scope: Node, raw: dict, global_context: Optional[dict]) -> Transition:
    raw = dict(raw)
    pos = raw.get("__pos__")  # (line, column) from the DSL loader, for located errors
    to = raw.get("to")
    sel = raw.get("selector")
    if to is None and sel is None:
        raise BuildError("invalid transition: both target and selector are undefined", pos)

    source = descend(scope, raw["from"])
    if source is None:
        raise BuildError(f"cannot resolve transition source {raw['from']!r} in {scope.full_path!r}", pos)

    target = None
    if to is not None:
        target = resolve_relative(scope, to)
        if target is None:
            raise BuildError(f"cannot resolve transition target {to!r} from scope {scope.full_path!r}", pos)

    return Transition(
        source=source,
        target=target,
        event_filter=_build_event_filter(raw["on_event"]) if raw.get("on_event") is not None else None,
        selector=_build_selector(sel, global_context) if sel is not None else None,
    )


def build_definition(
    config: dict,
    global_context: Optional[dict],
    name: str,
    definition_id: Optional[str] = None,
    validate: bool = False,
) -> Definition:
    config = dict(config)
    # the root is the StateMachine: parallel, empty full_path (children are unprefixed)
    root_norm = {**config, "name": name, "full_path": "", "type": NodeKind.PARALLEL.value}

    index: dict = {}
    pending: list = []
    submachines: dict = {}
    root = _build_node(root_norm, None, global_context, index, pending, submachines, name)
    for node, raw_transitions in pending:
        for raw in raw_transitions:
            node.transitions.append(_build_transition(node, raw, global_context))

    defn = Definition(
        id=definition_id or name,
        root=root,
        index=index,
        events=_build_events(config),
        submachines=submachines,
    )
    if validate:
        from harel.definition.validate import validate_or_raise

        validate_or_raise(defn)
    return defn
