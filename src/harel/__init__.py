"""harel — a hierarchical statechart (UML-style) engine.

Layers: `definition` (the immutable program, navigated by references), `dsl`
(the textual authoring surface that compiles to a Definition), `engine`
(Execution + the pure engine + drivers/runners), and `viz` (PlantUML). The
public API is re-exported here so callers stay decoupled from the internal
module layout.
"""

from harel.definition.validate import Issue, ValidationError, validate, validate_or_raise
from harel.dsl import DslError, definition_from_dsl, definition_from_dsl_file
from harel.dsl.resolve import FileResolver, ModuleResolver, SourceResolver
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution
from harel.engine.resolve import DictResolver, MachineResolver, ResolveError
from harel.engine.runtime import Driver
from harel.engine.store import DictStore, ExecutionStore, SqliteStore
from harel.faas import handler, http_action, lambda_action, openfaas_action, remote_action
from harel.idempotency import DictIdempotency, IdempotencyBackend, idempotent
from harel.spec.states import Action, Event, EventFilter, LogEvent, Selector, Transition
from harel.viz.plantuml import render

__all__ = [
    "Action",
    "Event",
    "EventFilter",
    "LogEvent",
    "Selector",
    "Transition",
    "render",
    "definition_from_dsl",
    "definition_from_dsl_file",
    "DslError",
    "validate",
    "validate_or_raise",
    "Issue",
    "ValidationError",
    "Execution",
    "Driver",
    "DurableRunner",
    "ExecutionStore",
    "DictStore",
    "SqliteStore",
    "MachineResolver",
    "DictResolver",
    "FileResolver",
    "ModuleResolver",
    "SourceResolver",
    "ResolveError",
    "idempotent",
    "IdempotencyBackend",
    "DictIdempotency",
    "lambda_action",
    "http_action",
    "openfaas_action",
    "remote_action",
    "handler",
]
