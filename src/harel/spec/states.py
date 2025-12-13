from __future__ import annotations  # noqa: F407

import datetime
import uuid
from typing import Callable, Dict, Optional, Union

import pydantic


def _new_id() -> str:
    """Generate a unique opaque id for a state-machine object."""
    return uuid.uuid4().hex


class Action(pydantic.BaseModel):
    """A user function referenced by a dotted string ``"module.function"`` (or a
    direct callable) plus bound ``inputs``. Pure data: the engine emits a
    reference and the runtime resolves/invokes it (see ``engine.runtime``)."""

    function: Union[str, Callable]
    inputs: Optional[dict] = None
    package: Optional[str] = None

    def __str__(self):
        return f"<Action {self.get_function_name()}>"

    def is_valid_format(self):
        return isinstance(self.function, str) and "." in self.function

    def get_function_name(self):
        if callable(self.function):
            return self.function.__name__
        elif self.is_valid_format():
            _, function_name = self.function.rsplit(".", maxsplit=1)
            return function_name
        else:
            return self.function


class Selector(Action):
    """
    Action used as a switch: the return value of the action is looked up in
    `mapper` to pick the target state name.
    """

    mapper: Dict[str, str]

    @pydantic.field_validator("mapper", mode="before")
    @classmethod
    def _stringify_mapper_keys(cls, mapper):
        # the selector looks up mapper[str(result)], so non-string keys (e.g. bool) are coerced
        if isinstance(mapper, dict):
            return {str(key): value for key, value in mapper.items()}
        return mapper

    def __str__(self):
        return f"<Selector {self.get_function_name()} on mapper {self.mapper}>"


class Event(pydantic.BaseModel):
    """A stimulus processed by a state machine."""

    id: str = pydantic.Field(default_factory=_new_id)  # dedupe key (at-least-once delivery)
    kind: str
    created_time: Optional[datetime.datetime] = None
    received_time: Optional[datetime.datetime] = None
    data: dict = {}

    def __str__(self):
        def truncate(value: str):
            return f"{value[:128]}... " if len(value) > 128 else value

        return f"<Event {self.kind} values {truncate(str(self.data))}>"


class EventFilter(pydantic.BaseModel):
    """A guard over events: `kind` (allows ``A | B``) plus a `data` dict of
    ``field__op -> value`` predicates and/or the composable ``all``/``any``/
    ``not`` combinators (interpreted by the engine)."""

    kind: str
    data: dict = {}

    def __str__(self):
        return f"<EventFilter {self.kind} values {self.data}>"


class Transition(pydantic.BaseModel):
    """A transition from `source` to either a static `target` or a `selector`,
    guarded by an optional `event_filter` (no filter => automatic)."""

    event_filter: Optional[EventFilter] = None
    source: str
    target: Optional[str] = None
    selector: Optional[Selector] = None

    def __str__(self):
        if self.selector is None:
            return f"<Transition from {self.source} to {self.target} on {self.event_filter}>"
        return f"<Transition from {self.source} to {self.selector}>"


class LogEvent(pydantic.BaseModel):
    """The result of processing one event, returned by the runtime. Tests assert
    `end_state` (the active position after processing)."""

    initial_state: Optional[str] = None
    event: Optional[Event] = None
    end_state: Optional[str] = None

    def __str__(self):
        evnt = f"Evt [{self.event.kind}] " if self.event else ""
        return evnt + f"Transitioned {self.initial_state} -> {self.end_state}"
