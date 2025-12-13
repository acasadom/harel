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

from dataclasses import dataclass, field

# Field types an event datum may declare. `any` opts out of type checking.
FIELD_TYPES = ("string", "int", "float", "bool", "any")

# Events the engine itself emits/consumes. They never need declaring and the
# validator never flags them as unknown.
RESERVED_EVENTS = frozenset({"Timeout", "Finished", "Cancel", "Reset", "SetState", "Start", "Returned"})


@dataclass(frozen=True)
class FieldSpec:
    """One field on an event's `data`."""

    type: str = "any"
    required: bool = True


@dataclass
class EventType:
    """A declared event: a name plus the schema of its `data` fields."""

    name: str
    fields: dict[str, FieldSpec] = field(default_factory=dict)
