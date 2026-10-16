"""Statechart DSL — a clear textual surface that compiles to a `Definition`.

`definition_from_dsl(text, name=None)` / `definition_from_dsl_file(path)` parse
the DSL, resolve imports + fragment includes, and build (optionally validate) the
same `Definition` the YAML front-end produces.
"""

from harel.dsl.loader import definition_from_dsl, definition_from_dsl_file, machine_version
from harel.dsl.parser import DslError, Program, parse

__all__ = [
    "definition_from_dsl",
    "definition_from_dsl_file",
    "machine_version",
    "parse",
    "Program",
    "DslError",
]
