"""Typed events for a `Definition`.

Today events are dynamic dicts; nothing declares what an event *is*. An
`EventType` declares an event's name and the schema of its `data` fields, so the
validator can check that a transition's `EventFilter` references a declared event
and that its predicates only touch fields that exist (with a compatible op).

This is surface-independent: the registry hangs off the `Definition`, populated
by whichever front-end declares events (today the optional YAML `events:` block;
tomorrow the DSL). Absent declarations => an empty registry => event validation
is skipped (back-compat).
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

# Field types an event datum may declare. `any` opts out of type checking.
FIELD_TYPES = ("string", "int", "float", "bool", "any")

# Events the engine itself emits/consumes. They never need declaring and the
# validator never flags them as unknown.
RESERVED_EVENTS = frozenset(
    {"Timeout", "Finished", "Cancel", "Reset", "SetState", "Start", "Returned", "error", "Expired"}
)


@dataclass(frozen=True)
class FieldSpec:
    """One declared field: of an event's `data`, or of a machine's context — which may give
    a `default`, the value an execution starts with when it isn't passed (None: no default;
    the DSL has no null literal)."""

    type: str = "any"
    required: bool = True
    default: Any = None


@dataclass
class EventType:
    """A declared event: a name plus the schema of its `data` fields."""

    name: str
    fields: dict[str, FieldSpec] = field(default_factory=dict)


def value_fits(type_: str, value: object) -> bool:
    """Whether `value` is of the declared field type. `int` excludes booleans; `float`
    accepts an int too."""
    if type_ == "any":
        return True
    if type_ == "string":
        return isinstance(value, str)
    if type_ == "bool":
        return isinstance(value, bool)
    if type_ == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_ == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return False


def schema_problems(schema: dict[str, FieldSpec], values: dict, *, check_required: bool = True) -> list[str]:
    """What in `values` breaks `schema`: a declared field of the wrong type (`None` is only
    accepted for an optional one), or — with `check_required` — a required field absent.
    Undeclared keys are allowed: the schema types what it names and leaves room for the
    rest (actions may keep their own keys)."""
    problems = []
    for name, spec in schema.items():
        if name not in values:
            if check_required and spec.required:
                problems.append(f"missing required field {name!r} ({spec.type})")
            continue
        value = values[name]
        if value is None and not spec.required:
            continue
        if not value_fits(spec.type, value):
            problems.append(f"field {name!r} must be {spec.type}, got {type(value).__name__} {value!r}")
    return problems


def with_defaults(schema: dict[str, FieldSpec], context: dict) -> dict:
    """`context` with every field of `schema` it lacks that declares a `default` added
    (a copy of the default, so a list default is never shared between executions)."""
    filled = dict(context)
    for name, spec in schema.items():
        if spec.default is not None and name not in filled:
            filled[name] = copy.deepcopy(spec.default)
    return filled


class ContextError(ValueError):
    """An execution context that doesn't fit its machine's declared `context` schema."""


def check_context(schema: dict[str, FieldSpec], context: dict, *, check_required: bool = True) -> None:
    """Raise `ContextError` if `context` doesn't fit `schema` (see `schema_problems`)."""
    problems = schema_problems(schema, context, check_required=check_required)
    if problems:
        raise ContextError("context doesn't fit the machine's `context` schema: " + "; ".join(problems))
