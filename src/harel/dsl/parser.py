"""Parse statechart DSL text into the normalized config dict.

The parser is pure syntax → structure: it produces a `Program` (imports + events
+ named machines/fragments), where each machine/fragment is the same config dict
shape `build_definition` consumes. `use` directives are left as a marker
(`__uses__`) for the loader to expand once all fragments are known.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from lark import Lark, Token, Transformer, v_args
from lark.exceptions import LarkError, UnexpectedCharacters, UnexpectedEOF, UnexpectedInput

_OPS = {"==": "eq", "!=": "ne", "<": "lt", "<=": "le", ">": "gt", ">=": "ge", "in": "in"}
_KIND_TYPE = {"orthogonal": "OrthogonalState"}

_GRAMMAR = (Path(__file__).parent / "grammar.lark").read_text()


class DslError(Exception):
    """A DSL parse/structure error. When a source position is known it renders a
    located, caret-annotated message:

        unbound action handler: send  (at line 4, column 12)

            on enter send
                       ^
        hint: declare it with `bind { send = pkg.mod.send }` or pass actions=

    The bare ``message`` is always the first line, so callers/tests that match on
    a substring keep working. ``line``/``column`` are 1-based; ``context`` is a
    pre-rendered snippet (the offending line + a caret); ``hint`` is optional
    guidance. Construct with just a message for the positionless case.
    """

    def __init__(
        self,
        message: str,
        *,
        line: Optional[int] = None,
        column: Optional[int] = None,
        context: Optional[str] = None,
        hint: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.line = line
        self.column = column
        self.context = context
        self.hint = hint

    def __str__(self) -> str:
        head = self.message
        if self.line is not None:
            loc = f"line {self.line}" + (f", column {self.column}" if self.column is not None else "")
            head = f"{self.message}  (at {loc})"
        parts = [head]
        if self.context:
            parts.append("\n" + self.context.rstrip("\n"))
        if self.hint:
            parts.append(f"hint: {self.hint}")
        return "\n".join(parts)


def _caret(source: Optional[str], line: Optional[int], column: Optional[int]) -> Optional[str]:
    """Render the offending source line with a `^` under `column` (1-based)."""
    if not source or line is None or line < 1:
        return None
    lines = source.splitlines()
    if line > len(lines):
        return None
    src_line = lines[line - 1]
    col = column if (column and column >= 1) else 1
    return f"    {src_line}\n    {' ' * (col - 1)}^"


@dataclass
class ActionSpec:
    """An unresolved action reference in the config: a bare `handler` name (bound
    separately) or a `literal` dotted path. The loader resolves it to a concrete
    function via the active bindings."""

    kind: str  # "handler" | "literal"
    ref: str
    inputs: dict = field(default_factory=dict)


@dataclass
class Program:
    # (path, alias, (line, column)) — position is the `import` statement's
    imports: list[tuple[str, Optional[str], tuple[int, int]]] = field(default_factory=list)
    bindings: dict[str, str] = field(default_factory=dict)  # handler name -> literal impl path
    guards: dict[str, dict] = field(default_factory=dict)  # guard name -> on_event predicate dict
    events: dict[str, dict] = field(default_factory=dict)
    machines: dict[str, dict] = field(default_factory=dict)
    fragments: dict[str, dict] = field(default_factory=dict)
    # declaration positions for tooling (the language server): ("event"|"guard"|
    # "fragment"|"machine", name) -> (line, column). Machines/fragments also carry
    # `__pos__` in their cfg; this map covers events/guards which do not.
    positions: dict[tuple[str, str], tuple[int, int]] = field(default_factory=dict)


def _unquote(s: str) -> str:
    return bytes(s[1:-1], "utf-8").decode("unicode_escape")


def _lit(tok: Token) -> Any:
    """Convert a BOOL / SIGNED_NUMBER token (STRING is unquoted by its terminal
    callback before it reaches here)."""
    s = str(tok)
    if tok.type == "BOOL":
        return s == "true"
    return float(s) if ("." in s or "e" in s or "E" in s) else int(s)


def _coerce(v: Any) -> Any:
    """A value leaf is a BOOL/number token, an already-unquoted string, or a list."""
    return _lit(v) if isinstance(v, Token) else v


def _pred_to_dict(node: tuple) -> dict:
    """Convert a predicate tree to the on_event dict shape. A pure AND of
    distinct-field leaves flattens to `{field__op: value}` (matches the YAML/flat
    representation); or/not/collisions become an `all`/`any`/`not` combinator."""
    tag = node[0]
    if tag == "leaf":
        _, fld, op, value = node
        return {f"{fld}__{_OPS[op]}": value}
    if tag == "all":
        dicts = [_pred_to_dict(c) for c in node[1]]
        merged: dict = {}
        for child, d in zip(node[1], dicts):
            if child[0] == "leaf" and set(d).isdisjoint(merged):
                merged.update(d)
            else:
                return {"all": dicts}
        return merged
    if tag == "any":
        return {"any": [_pred_to_dict(c) for c in node[1]]}
    if tag == "guardref":
        return {"__guard__": node[1]}  # a named-guard marker the loader resolves
    return {"not": _pred_to_dict(node[1])}  # neg


@v_args(inline=True)
class _ToProgram(Transformer):
    # --- leaves ---
    def NAME(self, t):  # noqa: N802
        return str(t)

    def STRING(self, t):  # noqa: N802
        return _unquote(str(t))

    def list(self, *items):
        return [_coerce(i) for i in items]

    def action(self, ref, *inputs):
        # action_ref is a NAME (-> str, a handler) or a DOTTED token (a literal path)
        kind = "literal" if isinstance(ref, Token) else "handler"
        return ActionSpec(kind, str(ref), dict(inputs))

    def input(self, key, value):
        return (str(key), value)

    def in_literal(self, v):
        return _coerce(v)

    def in_param(self, name):
        return {"__param__": str(name)}

    # --- action bindings ---
    def binding(self, name, impl):
        return (str(name), str(impl))

    def bind_block(self, *bindings):
        return ("bind", dict(bindings))

    # --- named guards ---
    @v_args(inline=True, meta=True)
    def guard_decl(self, meta, name, pred):
        return ("guard", str(name), _pred_to_dict(pred), (meta.line, meta.column))

    def guard_atom(self, name):
        # a named guard used as a predicate atom (alone or composed in a `where`);
        # carried as a marker the loader resolves against the active guards
        return ("guardref", str(name))

    # --- fragment parameters ---
    def param(self, name, kind):
        return (str(name), str(kind))

    def param_list(self, *params):
        return list(params)

    # --- events ---
    def field_decl(self, name, type_, optional=None):
        return (str(name), {"type": str(type_), "required": optional is None})

    @v_args(inline=True, meta=True)
    def event_decl(self, meta, name, *fields):
        return ("event", str(name), dict(fields), (meta.line, meta.column))

    # --- predicates ---
    def comparison(self, fld, op, value):
        return ("leaf", str(fld), str(op), _coerce(value))

    def all_expr(self, *children):
        return ("all", list(children))

    def any_expr(self, *children):
        return ("any", list(children))

    def neg(self, child):
        return ("not", child)

    def kinds(self, *names):
        return "|".join(str(n) for n in names)

    def trigger(self, kinds, where=None):
        # `where` is a predicate tree (a bare guard ref `where ok` is just a
        # single guard-atom predicate); guard markers are resolved by the loader
        d: dict = {"type": kinds}
        if where is not None:
            d.update(_pred_to_dict(where))
        return d

    # --- transitions ---
    @v_args(inline=True, meta=True)
    def transition(self, meta, src, tgt, trig=None):
        t: dict = {"from": str(src), "to": str(tgt), "__pos__": (meta.line, meta.column)}
        if trig is not None:
            t["on_event"] = trig
        return ("transition", t)

    def branch(self, value, tgt):
        # the engine looks a selector result up by str(result); a bool maps via
        # str(True)/str(False) ("True"/"False"), matching the YAML front-end.
        return ("branch", str(_coerce(value)), str(tgt))

    def else_branch(self, tgt):
        return ("else", str(tgt))

    def enum_decl(self, *values):
        return ("enum", [str(_coerce(v)) for v in values])

    def _join(self, src, mode, success_tgt, else_tgt, pos=None):
        # desugar `join all|any` to a selector over `region_results` using the
        # shipped aggregator: it returns "pass"/"fail", mapped to the two targets.
        action = ActionSpec("literal", "harel.lib.join_success", {"mode": mode})
        sel: dict = {"function": action, "mapper": {"pass": str(success_tgt)}, "default": str(else_tgt)}
        t: dict = {"from": str(src), "selector": sel}
        if pos is not None:
            t["__pos__"] = pos
        return ("transition", t)

    @v_args(inline=True, meta=True)
    def join_all(self, meta, src, success_tgt, else_tgt):
        return self._join(src, "all", success_tgt, else_tgt, (meta.line, meta.column))

    @v_args(inline=True, meta=True)
    def join_any(self, meta, src, success_tgt, else_tgt):
        return self._join(src, "any", success_tgt, else_tgt, (meta.line, meta.column))

    @v_args(inline=True, meta=True)
    def selector_trans(self, meta, src, action, *rest):
        enum = next((r[1] for r in rest if isinstance(r, tuple) and r[0] == "enum"), None)
        default = next((r[1] for r in rest if isinstance(r, tuple) and r[0] == "else"), None)
        trig = next((r for r in rest if isinstance(r, dict)), None)
        mapper = {k: v for tag, k, v in (r for r in rest if isinstance(r, tuple) and r[0] == "branch")}
        # `action` is an ActionSpec; the loader resolves it to a concrete function
        sel: dict = {"function": action, "mapper": mapper}
        if default is not None:
            sel["default"] = default
        if enum is not None:
            sel["enum"] = enum
        t: dict = {"from": str(src), "selector": sel, "__pos__": (meta.line, meta.column)}
        if trig is not None:
            t["on_event"] = trig
        return ("transition", t)

    # --- scalar state items ---
    def initial(self, name):
        return ("start", str(name))

    def on_enter(self, action):
        return ("on_enter", action)

    def on_exit(self, action):
        return ("on_exit", action)

    def on_activity(self, action):
        return ("on_activity", action)

    def timeout_literal(self, n):
        return int(n)

    def timeout_context(self, key):
        return {"context": str(key)}

    def timeout_param(self, name):
        return {"__param__": str(name)}

    def timeout(self, spec):
        return ("timeout", spec)

    def outcome(self, label):
        return ("outcome", str(label))

    def carry(self, *keys):
        return ("carry", tuple(str(k) for k in keys))

    def invoke_for(self, var, coll):
        return (str(var), str(coll))

    def inline_machine(self, *items):
        # an inline submachine body -> a machine config dict (built as its own
        # Definition with a synthetic FQN by the builder)
        return self._assemble(items)

    def invoke(self, *children):
        # children (any order): an FQN (str), an `invoke_for` (tuple), an inline body
        # (dict). One of FQN / inline is present.
        fqn = next((c for c in children if isinstance(c, str)), None)
        each = next((c for c in children if isinstance(c, tuple)), None)
        inline = next((c for c in children if isinstance(c, dict)), None)
        return ("invoke", fqn, each, inline)

    def with_pair(self, child_key, parent_key):
        return (str(child_key), str(parent_key))

    def invoke_with(self, *pairs):
        return ("invoke_with", dict(pairs))

    @v_args(inline=True, meta=True)
    def final_decl(self, meta, name, outcome, *items):
        # sugar: a terminal state with its verdict inline (`final Done success`);
        # any items are hooks (on enter/exit). Desugars to a leaf state + outcome.
        cfg = self._assemble(items)
        cfg["outcome"] = str(outcome)
        cfg["__pos__"] = (meta.line, meta.column)
        return ("state", str(name), cfg)

    def nohistory(self):
        return ("allow_history", False)

    # --- composition ---
    # a use arg is tagged by syntactic form; the loader interprets it per the
    # parameter's declared kind (action / state / guard / value)
    def a_name(self, n):
        return ("name", str(n))

    def a_dotted(self, d):
        return ("dotted", str(d))

    def a_value(self, v):
        return ("lit", _coerce(v))

    def a_pred(self, p):
        return ("pred", _pred_to_dict(p))

    def arg(self, name, tagged):
        return (str(name), tagged)

    def arg_list(self, *args):
        return dict(args)

    @v_args(inline=True, meta=True)
    def use_stmt(self, meta, name, *rest):
        args = next((r for r in rest if isinstance(r, dict)), {})
        alias = next((r for r in rest if isinstance(r, str)), None)
        return (
            "use",
            {"fragment": str(name), "alias": alias, "args": args, "__pos__": (meta.line, meta.column)},
        )

    @v_args(inline=True, meta=True)
    def import_stmt(self, meta, path, alias=None):
        return ("import", str(path), str(alias) if alias is not None else None, (meta.line, meta.column))

    # --- containers ---
    def _assemble(self, items) -> dict:
        cfg: dict[str, Any] = {}
        states: dict[str, Any] = {}
        transitions: list = []
        uses: list = []
        for kind, *rest in items:
            if kind == "state":
                states[rest[0]] = rest[1]
            elif kind == "transition":
                transitions.append(rest[0])
            elif kind == "use":
                uses.append(rest[0])
            elif kind == "invoke":
                if rest[0] is not None:  # FQN target
                    cfg["invoke"] = rest[0]
                if rest[1] is not None:  # `for V in COLL` -> a fan-out
                    cfg["invoke_each"] = rest[1]
                if rest[2] is not None:  # an inline machine body
                    cfg["invoke_inline"] = rest[2]
            else:
                cfg[kind] = rest[0]
        if states:
            cfg["states"] = states
        if transitions:
            cfg["transitions"] = transitions
        if uses:
            cfg["__uses__"] = uses
        return cfg

    @v_args(inline=True, meta=True)
    def state_decl(self, meta, kind, name, *items):
        cfg = self._assemble(items)
        cfg["__pos__"] = (meta.line, meta.column)
        if str(kind) in _KIND_TYPE:
            cfg["type"] = _KIND_TYPE[str(kind)]
            # regions of an AND-state are parallel composites; promote untyped
            # composite children so the DSL needs no separate `parallel` keyword
            for child in cfg.get("states", {}).values():
                if isinstance(child, dict) and "states" in child and "type" not in child:
                    child["type"] = "ParallelState"
        return ("state", str(name), cfg)

    @v_args(inline=True, meta=True)
    def machine_decl(self, meta, name, *items):
        cfg = self._assemble(items)
        cfg["__pos__"] = (meta.line, meta.column)
        return ("machine", str(name), cfg)

    @v_args(inline=True, meta=True)
    def fragment_decl(self, meta, name, *rest):
        params = rest[0] if rest and isinstance(rest[0], list) else []
        items = rest[1:] if params else rest
        cfg = self._assemble(items)
        if params:
            cfg["__params__"] = params
        cfg["__pos__"] = (meta.line, meta.column)
        return ("fragment", str(name), cfg)

    def start(self, *stmts):
        prog = Program()
        for kind, *rest in stmts:
            if kind == "import":
                prog.imports.append((rest[0], rest[1], rest[2]))
            elif kind == "bind":
                prog.bindings.update(rest[0])
            elif kind == "guard":
                prog.guards[rest[0]] = rest[1]
                prog.positions[("guard", rest[0])] = rest[2]
            elif kind == "event":
                prog.events[rest[0]] = rest[1]
                prog.positions[("event", rest[0])] = rest[2]
            elif kind == "machine":
                prog.machines[rest[0]] = rest[1]
                prog.positions[("machine", rest[0])] = rest[1]["__pos__"]
            elif kind == "fragment":
                prog.fragments[rest[0]] = rest[1]
                prog.positions[("fragment", rest[0])] = rest[1]["__pos__"]
        return prog


_PARSER = Lark(_GRAMMAR, parser="earley", propagate_positions=True)


def _syntax_hint(text: str, line: Optional[int]) -> Optional[str]:
    """A best-effort suggestion for the most common syntax slips, keyed off the
    offending source line (earley reports these as bare `UnexpectedCharacters`
    with no token/expected info, so we inspect the line itself)."""
    if line is None or line < 1:
        return None
    lines = text.splitlines()
    if line > len(lines):
        return None
    src = lines[line - 1]
    if "->" in src or "-->" in src:
        return "transitions read `from A to B` — there is no `->` arrow"
    if "=>" in src:
        return "use `to` for a target and `select`/`{ ... }` for a selector — there is no `=>`"
    return None


def _syntax_error(text: str, e: UnexpectedInput) -> DslError:
    """Turn a lark parse failure into a located, caret-annotated `DslError`."""
    line, column = e.line, e.column
    if isinstance(e, UnexpectedEOF) or line is None or line < 1:
        # earley reports EOF at line -1; point at the end of the source instead
        src_lines = text.splitlines() or [""]
        line, column = len(src_lines), len(src_lines[-1]) + 1
        message = "unexpected end of input (a `{` block is likely missing its closing `}`)"
    elif isinstance(e, UnexpectedCharacters):
        message = "unexpected input here"
    else:
        tok = getattr(e, "token", None)
        message = f"unexpected {str(tok)!r}" if tok else "syntax error"
    context: Optional[str]
    try:
        context = e.get_context(text).rstrip("\n")
    except Exception:
        context = _caret(text, line, column)
    return DslError(message, line=line, column=column, context=context, hint=_syntax_hint(text, line))


def parse(text: str) -> Program:
    """Parse DSL text into a `Program`. Raises `DslError` (with source position
    and a caret-annotated snippet) on malformed input."""
    try:
        tree = _PARSER.parse(text)
    except UnexpectedInput as e:
        raise _syntax_error(text, e) from e
    except LarkError as e:
        raise DslError(f"DSL syntax error: {e}") from e
    return _ToProgram().transform(tree)
