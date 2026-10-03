"""The readable text of a transition's guard, shared by the PlantUML and Mermaid renderers.

A guard has two parts that are AND-ed: the flat `field__op -> value` dict and the composable
predicate tree (`all`/`any`/`not`, which named guards and `and`/`or`/`not` expressions build).
Both are rendered, so a guard never silently disappears from a diagram."""

from __future__ import annotations

from typing import Any, Optional

from harel.definition.model import Assign, EventFilter, Expr, Predicate

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
        field = pred.field or ""
        left = field if pred.source == "event" else f"{pred.source}.{field}"
        if pred.value_ref is not None:  # a reference on the right, written namespaced
            return f"{left} {_OPS.get(pred.op or 'eq', pred.op)} {_expr(pred.value_ref)}"
        return _comparison(left, pred.op or "eq", pred.value)
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


def branch_text(guard: Predicate) -> str:
    """A `choose` branch's guard as text."""
    return _predicate(guard)


def _expr(expr: Expr) -> str:
    if expr.kind == "ref":
        return f"{expr.source}.{expr.field}"
    if expr.kind == "lit":
        return _value(expr.value)
    assert expr.left is not None and expr.right is not None
    return f"{_expr(expr.left)} {expr.op} {_expr(expr.right)}"


def effect_text(assignments: tuple[Assign, ...]) -> Optional[str]:
    """A transition's `set` as text, comma-separated as written in the DSL
    (`context.n = context.n + 1, context.last = event.id`)."""
    if not assignments:
        return None
    return ", ".join(f"context.{a.field} = {_expr(a.expr)}" for a in assignments)
