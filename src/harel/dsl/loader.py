"""Resolve a DSL `Program` into a `Definition`.

Resolution steps the parser leaves to the loader (they need cross-file /
cross-fragment / binding knowledge):

- **imports** (`import "lib.stm" [as ns]`): parse the imported file and merge its
  fragments (prefixed by `ns.` when aliased), events, bindings and guards.
- **bindings** (`bind { handler = pkg.fn }` + the programmatic `actions=`): every
  action is a bare *handler* name or a literal dotted path; bindings map handlers
  to concrete impls (the "swap the actions" seam; `actions=` wins).
- **named guards** (`guard ok = status == "x"`): a `where ok` references it; the
  loader substitutes the predicate. Unbound guard => `DslError`.
- **parametrized fragments + uses**: a fragment declares a customizable
  surface `fragment F(work: action, give_up: state, ok: guard, budget: value)
  { ... }`; a `use F(work = ..., give_up = ..., ok = ..., budget = ...) as
  Local` fills it. action args bind handlers, state args substitute transition
  targets (resolved in the consumer's scope), guard args supply predicates, value
  args substitute `__param__` references in `timeout`s and action inputs — all
  scoped to that instance.

Every reference is resolved into the concrete form `build_definition` consumes; an
unbound handler / guard / value param is a hard `DslError`.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Union

from harel.definition.builder import BuildError, build_definition
from harel.definition.model import Definition
from harel.dsl.parser import ActionSpec, DslError, _caret, parse

Impl = Union[str, Callable]  # a literal dotted path, or (programmatic only) a callable


@dataclass
class _Ctx:
    """The resolution scope threaded through expansion: the maps to resolve
    against, plus the accumulating sets of unresolved references."""

    bindings: dict[str, Impl]
    guards: dict[str, dict]
    values: dict[str, Any]
    unbound: set[str] = field(default_factory=set)  # action handlers
    unbound_g: set[str] = field(default_factory=set)  # guards
    unbound_v: set[str] = field(default_factory=set)  # value params
    source: Optional[str] = None  # the DSL text of the file this scope's nodes live in (for error carets)

    def instance(self, bindings: dict, guards: dict, values: dict, source: Optional[str] = None) -> "_Ctx":
        """A child scope for a use: fresh maps, shared unbound sets. `source` is the
        fragment's own file text when it came from an import (else the parent's)."""
        return _Ctx(
            bindings, guards, values, self.unbound, self.unbound_g, self.unbound_v, source or self.source
        )


def _located(message: str, node: Any, ctx: _Ctx, hint: Optional[str] = None) -> DslError:
    """Build a `DslError` carrying the source position stashed on `node` (`__pos__`,
    set by the parser) plus a caret snippet rendered against this scope's file."""
    pos = node.get("__pos__") if isinstance(node, dict) else None
    if pos:
        return DslError(message, line=pos[0], column=pos[1], context=_caret(ctx.source, *pos), hint=hint)
    return DslError(message, hint=hint)


def _subst_value(v: Any, ctx: _Ctx) -> Any:
    """Replace a `{__param__: name}` value-parameter reference with its literal."""
    if isinstance(v, dict) and "__param__" in v:
        name = v["__param__"]
        if name in ctx.values:
            return ctx.values[name]
        ctx.unbound_v.add(name)
    return v


def _resolve_action(spec: ActionSpec, ctx: _Ctx) -> dict:
    """An ActionSpec -> the `{function, inputs}` config form: resolve the handler
    via bindings and substitute any value-param references in the inputs."""
    if spec.kind == "literal":
        fn: Impl = spec.ref
    elif spec.ref in ctx.bindings:
        fn = ctx.bindings[spec.ref]
    else:
        ctx.unbound.add(spec.ref)
        fn = spec.ref
    return {"function": fn, "inputs": {k: _subst_value(v, ctx) for k, v in spec.inputs.items()}}


def _resolve_guards(pred: dict, ctx: _Ctx) -> dict:
    """Recursively replace `{"__guard__": name}` markers in an on_event predicate
    dict with the named guard's predicate. Handles a bare reference (`where ok`,
    the marker sits beside `type`) and composition (`where ok and status == "x"`,
    the marker is nested inside an `all`/`any`/`not` combinator)."""
    out: dict = {}
    for key, value in pred.items():
        if key == "__guard__":
            if value in ctx.guards:
                out.update(_resolve_guards(ctx.guards[value], ctx))
            else:
                ctx.unbound_g.add(value)
        elif key in ("all", "any"):
            out[key] = [_resolve_guards(c, ctx) for c in value]
        elif key == "not":
            out[key] = _resolve_guards(value, ctx)
        else:
            out[key] = value
    return out


def _resolve_trans(t: dict, ctx: _Ctx) -> dict:
    """Resolve a transition's selector action and any named-guard markers."""
    sel = t.get("selector")
    if sel and isinstance(sel.get("function"), ActionSpec):
        t = {**t, "selector": {**sel, **_resolve_action(sel["function"], ctx)}}
    oe = t.get("on_event")
    if oe is not None:
        t = {**t, "on_event": _resolve_guards(oe, ctx)}
    return t


def _substitute_targets(cfg: dict, subst: dict[str, str]) -> dict:
    """Rename transition / selector targets that equal a state-parameter name."""
    if not subst:
        return cfg
    cfg = dict(cfg)
    if "transitions" in cfg:
        out = []
        for t in cfg["transitions"]:
            t = dict(t)
            if t.get("to") in subst:
                t["to"] = subst[t["to"]]
            if isinstance(t.get("selector"), dict):
                sel = dict(t["selector"])
                sel["mapper"] = {k: subst.get(v, v) for k, v in sel["mapper"].items()}
                t["selector"] = sel
            out.append(t)
        cfg["transitions"] = out
    if "states" in cfg:
        cfg["states"] = {n: _substitute_targets(s, subst) for n, s in cfg["states"].items()}
    return cfg


def _substitute_events(cfg: dict, subst: dict[str, str]) -> dict:
    """Rename transition trigger kinds (`on <param>`) that equal an event-parameter
    name — applied per `|`-separated kind, so a param can sit among other kinds."""
    if not subst:
        return cfg
    cfg = dict(cfg)
    if "transitions" in cfg:
        out = []
        for t in cfg["transitions"]:
            oe = t.get("on_event")
            if isinstance(oe, dict) and "type" in oe:
                kinds = [subst.get(k.strip(), k.strip()) for k in oe["type"].split("|")]
                t = {**t, "on_event": {**oe, "type": "|".join(kinds)}}
            out.append(t)
        cfg["transitions"] = out
    if "states" in cfg:
        cfg["states"] = {n: _substitute_events(s, subst) for n, s in cfg["states"].items()}
    return cfg


def _instantiate(inc: dict, fragments: dict[str, dict], ctx: _Ctx) -> tuple[str, dict]:
    """Materialize a `use`: validate args against the fragment's params, build
    the instance scope (bindings/guards/values + target substitutions), and expand."""
    name = inc["fragment"]
    if name not in fragments:
        raise _located(f"use of unknown fragment {name!r}", inc, ctx)
    frag = dict(fragments[name])
    frag_src = frag.pop("__src__", ctx.source)  # the fragment's own file (imports), for inner carets
    params = dict(frag.pop("__params__", []))
    args = inc["args"]
    if set(args) - set(params):
        raise _located(f"use {name!r}: unknown args: {', '.join(sorted(set(args) - set(params)))}", inc, ctx)
    if set(params) - set(args):
        raise _located(f"use {name!r}: missing args: {', '.join(sorted(set(params) - set(args)))}", inc, ctx)

    bindings = dict(ctx.bindings)
    guards = dict(ctx.guards)
    values = dict(ctx.values)
    subst: dict[str, str] = {}
    event_subst: dict[str, str] = {}
    for pname, kind in params.items():
        tag, payload = args[pname]
        if kind == "action":
            if tag == "dotted":
                bindings[pname] = payload
            elif tag == "name":  # forward a parent handler
                if payload not in ctx.bindings:
                    ctx.unbound.add(payload)
                bindings[pname] = ctx.bindings.get(payload, payload)
            else:
                raise _located(f"use {name!r}: arg {pname!r} must be an action handler or path", inc, ctx)
        elif kind == "state":
            if tag in ("name", "dotted"):
                subst[pname] = payload
            else:
                raise _located(f"use {name!r}: arg {pname!r} must be a state name", inc, ctx)
        elif kind == "event":
            if tag in ("name", "dotted"):
                event_subst[pname] = payload
            else:
                raise _located(f"use {name!r}: arg {pname!r} must be an event name", inc, ctx)
        elif kind == "guard":
            if tag == "pred":
                guards[pname] = payload
            elif tag == "name" and payload in ctx.guards:
                guards[pname] = ctx.guards[payload]
            elif tag == "name":
                ctx.unbound_g.add(payload)
            else:
                raise _located(f"use {name!r}: arg {pname!r} must be a guard predicate or name", inc, ctx)
        else:  # value
            if tag in ("lit", "name"):
                values[pname] = payload
            else:
                raise _located(f"use {name!r}: arg {pname!r} must be a literal value", inc, ctx)

    local = inc["alias"] or name.split(".")[-1]
    clone = _substitute_events(_substitute_targets(copy.deepcopy(frag), subst), event_subst)
    return local, _expand(clone, fragments, ctx.instance(bindings, guards, values, frag_src))


def _expand(cfg: dict, fragments: dict[str, dict], ctx: _Ctx) -> dict:
    """Resolve a composite's hooks / selectors / guards / timeout and value-param
    references, splice its `__uses__` as child composites, and recurse."""
    cfg = dict(cfg)
    uses = cfg.pop("__uses__", [])
    for hook in ("on_enter", "on_exit", "on_activity"):
        if isinstance(cfg.get(hook), ActionSpec):
            cfg[hook] = _resolve_action(cfg[hook], ctx)
    if "timeout" in cfg:
        cfg["timeout"] = _subst_value(cfg["timeout"], ctx)
    if "transitions" in cfg:
        cfg["transitions"] = [_resolve_trans(t, ctx) for t in cfg["transitions"]]
    if "invoke_inline" in cfg:  # an inline submachine body resolves in this scope
        cfg["invoke_inline"] = _expand(cfg["invoke_inline"], fragments, ctx)

    states = {n: _expand(s, fragments, ctx) for n, s in cfg.get("states", {}).items() if isinstance(s, dict)}
    for inc in uses:
        local, expanded = _instantiate(inc, fragments, ctx)
        if local in states:
            raise _located(f"use {inc['fragment']!r} as {local!r} collides with an existing state", inc, ctx)
        states[local] = expanded
    if states:
        cfg["states"] = states
    return cfg


def _load_program(text: str, base: Optional[Path], seen: set[Path]):
    """Parse `text` and recursively pull imported events, fragments, bindings and
    guards into merged registries (own declarations override imported ones)."""
    prog = parse(text)
    events: dict[str, dict] = {}
    fragments: dict[str, dict] = {}
    bindings: dict[str, str] = {}
    guards: dict[str, dict] = {}
    for path, alias, pos in prog.imports:
        resolved = ((base / path) if base is not None else Path(path)).resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if not resolved.exists():
            raise DslError(
                f"import not found: {path}", line=pos[0], column=pos[1], context=_caret(text, *pos)
            )
        _, ie, ifr, ib, ig = _load_program(resolved.read_text(), resolved.parent, seen)
        events.update(ie)
        bindings.update(ib)
        guards.update(ig)
        for fname, fcfg in ifr.items():
            fragments[f"{alias}.{fname}" if alias else fname] = fcfg
    events.update(prog.events)
    bindings.update(prog.bindings)
    guards.update(prog.guards)
    for fcfg in prog.fragments.values():  # tag own fragments with this file's text (imports keep theirs)
        fcfg.setdefault("__src__", text)
    fragments.update(prog.fragments)
    return prog, events, fragments, bindings, guards


def definition_from_dsl(
    text: str,
    name: Optional[str] = None,
    *,
    base_path: Optional[Path] = None,
    actions: Optional[dict[str, Impl]] = None,
    guards: Optional[dict[str, dict]] = None,
    validate: bool = False,
    _register_imports: bool = True,
) -> Definition:
    """Build a `Definition` from DSL text. `name` selects the machine (required
    only if the program declares more than one). `base_path` is the directory
    imports resolve against. `actions` binds handler names to concrete impls
    (dotted path or callable), overriding any in-DSL `bind` defaults. `guards`
    binds guard names to predicate dicts (the on_event predicate shape, e.g.
    `{"status__eq": "x"}` or `{"all": [...]}`), overriding any in-DSL `guard`
    decls — the symmetric seam for guards.

    `import`ed files' **machines** are built and registered as resolvable
    submachines (keyed by their alias-scoped FQN), so a black-box `invoke <fqn>`
    of a machine defined in an imported file resolves without an external resolver
    (the same seam inline `invoke` uses; an injected resolver still overrides).
    """
    cfg, name, has_imports = _resolve_cfg(text, name, base_path, actions, guards)
    defn = _build(cfg, name, text, has_imports, validate=validate)
    if _register_imports:
        for fqn, sub in _imported_machines(text, base_path, set()).items():
            defn.submachines.setdefault(fqn, sub)
    return defn


def _imported_machines(text: str, base: Optional[Path], seen: set[Path]) -> dict[str, Definition]:
    """Build every machine of each `import`ed file into a `Definition`, keyed by
    its alias-scoped FQN (`{alias}.{name}`, else `{name}`) so a consumer's
    `invoke` resolves it by exact FQN. Recurses through transitive imports (alias
    chain preserved) and is cycle-safe via `seen` (resolved file paths). A file
    that fails to parse/build is skipped — the main build already reported it."""
    out: dict[str, Definition] = {}
    try:
        prog = parse(text)
    except DslError:
        return out
    for path, alias, _pos in prog.imports:
        resolved = ((base / path) if base is not None else Path(path)).resolve()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        itext = resolved.read_text()
        try:
            imachines = list(parse(itext).machines)
        except DslError:
            continue
        for mname in imachines:
            fqn = f"{alias}.{mname}" if alias else mname
            try:
                sub = definition_from_dsl(itext, mname, base_path=resolved.parent, _register_imports=False)
            except DslError:
                continue
            sub.id = fqn  # so the runner registers (and resolves) it by this FQN
            out[fqn] = sub
            out.update(sub.submachines)  # flatten the imported machine's own inline subs
        for tfqn, tsub in _imported_machines(itext, resolved.parent, seen).items():
            out.setdefault(f"{alias}.{tfqn}" if alias else tfqn, tsub)
    return out


def _resolve_cfg(
    text: str,
    name: Optional[str],
    base_path: Optional[Path],
    actions: Optional[dict[str, Impl]],
    guards: Optional[dict[str, dict]],
) -> tuple[dict, str, bool]:
    """Parse + load imports, select the machine, expand it, and check for unbound
    references. Returns the config dict `build_definition` consumes, the resolved
    machine name, and whether the program had imports (so callers know a source
    snippet against `text` may be unreliable for spliced nodes)."""
    prog, events, fragments, bindings, dsl_guards = _load_program(text, base_path, set())
    if not prog.machines:
        raise DslError("no machine declared")
    if name is None:
        if len(prog.machines) > 1:
            raise DslError(f"multiple machines ({', '.join(prog.machines)}); pass name=")
        name = next(iter(prog.machines))
    elif name not in prog.machines:
        raise DslError(f"no machine named {name!r}")

    machine = prog.machines[name]
    ctx = _Ctx(
        bindings={**bindings, **(actions or {})},
        guards={**dsl_guards, **(guards or {})},
        values={},
        source=text,
    )
    cfg = _expand(machine, fragments, ctx)
    # unbound references are aggregated across the whole machine (no single site), so
    # they point at the machine declaration as a fallback anchor.
    if ctx.unbound:
        raise _located(
            f"unbound action handlers: {', '.join(sorted(ctx.unbound))} (add a `bind` or pass actions=)",
            machine,
            ctx,
        )
    if ctx.unbound_g:
        raise _located(
            f"unbound guards: {', '.join(sorted(ctx.unbound_g))} "
            "(add a `guard` decl, a use arg, or pass guards=)",
            machine,
            ctx,
        )
    if ctx.unbound_v:
        raise _located(
            f"unbound value params: {', '.join(sorted(ctx.unbound_v))} (only valid inside a fragment)",
            machine,
            ctx,
        )
    if events:
        cfg = {**cfg, "events": {**events, **cfg.get("events", {})}}
    return cfg, name, bool(prog.imports)


def _build(cfg: dict, name: str, text: str, has_imports: bool, *, validate: bool = False) -> Definition:
    """Build a Definition from a resolved cfg, re-raising the builder's transition
    resolution failures as located DslErrors (the transition carries its `__pos__`)."""
    try:
        return build_definition(cfg, {}, name, validate=validate)
    except BuildError as e:
        pos = getattr(e, "pos", None)
        if pos:
            # the snippet is reliable only for a single-file program (no imported sources)
            context = _caret(text, *pos) if not has_imports else None
            raise DslError(str(e), line=pos[0], column=pos[1], context=context) from e
        raise DslError(str(e)) from e


def _position_index(cfg: dict, path: str = "", out: Optional[dict] = None) -> dict[str, tuple[int, int]]:
    """`full_path -> (line, column)` for every state in an expanded machine cfg,
    mirroring how `build_definition` derives full_paths (root is ""). Lets tooling
    map a `validate()` finding (which references a node by `full_path`) back to its
    source. A state spliced from an imported fragment carries that file's position."""
    out = {} if out is None else out
    pos = cfg.get("__pos__")
    if pos is not None:
        out[path] = (pos[0], pos[1])
    for cname, st in (cfg.get("states") or {}).items():
        if isinstance(st, dict):
            child = f"{path}.{cname}" if path else cname
            _position_index(st, child, out)
    return out


def definition_with_positions(
    text: str, name: Optional[str] = None, *, base_path: Optional[Path] = None
) -> tuple[Definition, dict[str, tuple[int, int]]]:
    """For tooling (the language server): build the Definition AND a
    `full_path -> (line, column)` index, so `validate()` findings can be mapped back
    to the source state. Positions of states spliced from an imported fragment refer
    to that fragment's file, not `text`."""
    cfg, name, has_imports = _resolve_cfg(text, name, base_path, None, None)
    positions = _position_index(cfg)
    return _build(cfg, name, text, has_imports), positions


def definition_from_dsl_file(
    path: str | Path,
    name: Optional[str] = None,
    *,
    actions: Optional[dict[str, Impl]] = None,
    guards: Optional[dict[str, dict]] = None,
    validate: bool = False,
) -> Definition:
    """Load + build from a `.stm` file (imports resolve relative to its dir)."""
    p = Path(path)
    return definition_from_dsl(
        p.read_text(), name, base_path=p.parent, actions=actions, guards=guards, validate=validate
    )
