"""The readable text of a transition's guard, shared by the PlantUML and Mermaid renderers.

A guard has two parts that are AND-ed: the flat `field__op -> value` dict and the composable
predicate tree (`all`/`any`/`not`, which named guards and `and`/`or`/`not` expressions build).
Both are rendered, so a guard never silently disappears from a diagram."""

from __future__ import annotations

from typing import Any, Optional

from harel.definition.model import EventFilter, Predicate

_OPS = {"eq": "==", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">=", "in": "in"}


def _value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"  # as written in the DSL
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, list):
        return "[" + ", ".join(_value(v) for v in value) + "]"
    return str(value)


def _comparison(field: str, op: str, value: Any) -> str:
    return f"{field} {_OPS.get(op, op)} {_value(value)}"


def _predicate(pred: Predicate, parent: Optional[str] = None) -> str:
    """`pred` as text. `and` binds tighter than `or` and `not` tightest, but an `and` inside
    an `or` is parenthesized anyway — `a and b or c` is correct yet easy to misread."""
    if pred.node == "leaf":
        return _comparison(pred.field or "", pred.op or "eq", pred.value)
    if pred.node == "not":
        return "not " + _predicate(pred.children[0], "not")
    joiner = " and " if pred.node == "all" else " or "
    text = joiner.join(_predicate(c, pred.node) for c in pred.children)
    needs_parens = parent == "not" or (parent is not None and parent != pred.node)
    return f"({text})" if needs_parens else text


def guard_text(ef: Optional[EventFilter]) -> Optional[str]:
    """The guard of `ef` as text (`status == 'paid' and (n > 1 or retry == true)`), or None
    if it has none."""
    if ef is None:
        return None
    parts = []
    for key, value in ef.predicates.items():
        field, sep, op = key.rpartition("__")
        parts.append(_comparison(field, op, value) if sep else _comparison(key, "eq", value))
    if ef.predicate is not None:
        tree = _predicate(ef.predicate, "all" if parts else None)
        parts.append(tree)
    return " and ".join(parts) if parts else None
