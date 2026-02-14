"""Symbol index + cursor resolution for the language server — no LSP dependency.

`index(text)` walks a DSL document and collects its declarations (states, events,
guards, fragments) with their source position and a markdown hover detail.
`word_at(text, line, character)` returns the identifier under a 0-based cursor and
the category it is referenced as (state / event / guard / fragment), inferred from
the preceding keyword — enough for go-to-definition, hover and context-aware
completion. Both are pure, so they are tested without `pygls`.

Scope (v1): symbols declared in the document itself. Imported events / fragments
are not followed to their file; quoted state names (`"Place Order"`) are matched
by their inner text. Positions are 1-based (matching `DslError`); `word_at` works
in 0-based LSP coordinates and the server converts.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from harel.definition.model import Definition, Node, resolve_relative
from harel.dsl.loader import definition_with_positions
from harel.dsl.parser import DslError, parse

# the keyword immediately before a reference tells us what it refers to
_STATE_KW = {"to", "from", "initial"}
_EVENT_KW = {"on"}
_FRAGMENT_KW = {"use"}
_GUARD_KW = {"where"}
_INVOKE_KW = {"invoke"}  # `invoke <fqn>` references a machine (local or imported)

# offered by completion regardless of context
KEYWORDS = [
    "machine",
    "fragment",
    "state",
    "orthogonal",
    "final",
    "initial",
    "from",
    "to",
    "on",
    "enter",
    "exit",
    "activity",
    "where",
    "select",
    "returns",
    "join",
    "all",
    "any",
    "else",
    "invoke",
    "for",
    "in",
    "with",
    "use",
    "as",
    "import",
    "event",
    "guard",
    "bind",
    "timeout",
    "context",
    "outcome",
    "carry",
    "no",
    "history",
]


@dataclass(frozen=True)
class Symbol:
    """A declaration: its bare `name`, `kind`, 1-based source position, a markdown
    `detail` shown on hover / completion, and the `uri` of the file it is declared
    in (None = the open document; set for symbols pulled in from an `import`)."""

    name: str
    kind: str  # "state" | "event" | "guard" | "fragment" | "machine"
    line: int
    column: int
    detail: str
    uri: Optional[str] = None


@dataclass
class SymbolIndex:
    states: dict[str, list[Symbol]] = field(default_factory=dict)  # leaf name -> symbols (scopes repeat)
    states_by_path: dict[str, Symbol] = field(default_factory=dict)
    events: dict[str, Symbol] = field(default_factory=dict)
    guards: dict[str, Symbol] = field(default_factory=dict)
    fragments: dict[str, Symbol] = field(default_factory=dict)
    machines: dict[str, Symbol] = field(default_factory=dict)  # invoke FQN (alias-scoped) -> decl
    definitions: dict[str, Definition] = field(default_factory=dict)  # machine name -> built Definition

    def lookup(self, name: str, category: Optional[str] = None) -> Optional[Symbol]:
        """Resolve `name` to a declaration. With a `category` only that kind is
        searched; without one, the kinds are tried in turn (state, event, guard,
        fragment). For a state name that repeats across scopes the first wins."""
        order = [category] if category else ["state", "event", "guard", "fragment"]
        for cat in order:
            if cat == "state" and self.states.get(name):
                return self.states[name][0]
            if cat == "event" and name in self.events:
                return self.events[name]
            if cat == "guard" and name in self.guards:
                return self.guards[name]
            if cat == "fragment" and name in self.fragments:
                return self.fragments[name]
            if cat in ("invoke", "machine") and name in self.machines:
                return self.machines[name]
        return None

    def names(self, category: Optional[str]) -> list[Symbol]:
        """All symbols of a category (for completion); without one, everything."""
        states = [syms[0] for syms in self.states.values()]
        if category == "state":
            return states
        if category == "event":
            return list(self.events.values())
        if category == "guard":
            return list(self.guards.values())
        if category == "fragment":
            return list(self.fragments.values())
        if category == "invoke":
            return list(self.machines.values())
        return (
            states + list(self.events.values()) + list(self.guards.values()) + list(self.fragments.values())
        )


def _action(a) -> str:
    return a.function if a is not None else ""


def _state_detail(node: Node) -> str:
    lines = [f"**state {node.name or '<root>'}** ({node.kind.name.lower()})"]
    if node.children:
        lines.append("children: " + ", ".join(c.name for c in node.children))
    for label, hook in (
        ("on enter", node.on_enter),
        ("on exit", node.on_exit),
        ("on activity", node.on_activity),
    ):
        if hook is not None:
            lines.append(f"{label}: `{_action(hook)}`")
    if node.timeout is not None:
        lines.append(f"timeout: `{node.timeout}`")
    if node.outcome is not None:
        lines.append(f"outcome: `{node.outcome}`")
    if node.invoke is not None:
        lines.append(f"invoke: `{node.invoke}`")
    return "\n\n".join(lines)


def _event_detail(name: str, fields: dict) -> str:
    if not fields:
        return f"**event {name}**"
    parts = [
        f"{f}{'' if spec.get('required', True) else '?'}: {spec.get('type', 'any')}"
        for f, spec in fields.items()
    ]
    return f"**event {name}**\n\nfields: " + ", ".join(parts)


def _guard_detail(name: str, pred: dict) -> str:
    return f"**guard {name}**\n\n`{pred}`"


def _fragment_detail(name: str, cfg: dict) -> str:
    params = cfg.get("__params__", [])
    sig = ", ".join(f"{p}: {k}" for p, k in params)
    return f"**fragment {name}**({sig})"


def _machine_detail(name: str, cfg: dict) -> str:
    lines = [f"**machine {name}** (invoke target)"]
    if cfg.get("start"):
        lines.append(f"initial: `{cfg['start']}`")
    return "\n\n".join(lines)


def _collect_decls(prog, idx: SymbolIndex, uri: Optional[str], prefix: str = "") -> None:
    """Add a program's event / guard / fragment / machine declarations to the index
    (events and guards keep their bare names; fragments and machines take the import
    `prefix`, mirroring the loader's `alias.Frag` / `alias.machine` namespacing — so a
    machine is keyed by the FQN an `invoke` would use). Later callers override earlier
    ones, so local declarations (collected last) win over imported."""
    for name, fields in prog.events.items():
        pos = prog.positions.get(("event", name))
        if pos:
            idx.events[name] = Symbol(name, "event", pos[0], pos[1], _event_detail(name, fields), uri)
    for name, pred in prog.guards.items():
        pos = prog.positions.get(("guard", name))
        if pos:
            idx.guards[name] = Symbol(name, "guard", pos[0], pos[1], _guard_detail(name, pred), uri)
    for name, cfg in prog.fragments.items():
        pos = prog.positions.get(("fragment", name))
        if pos:
            key = prefix + name
            idx.fragments[key] = Symbol(key, "fragment", pos[0], pos[1], _fragment_detail(name, cfg), uri)
    for name, cfg in prog.machines.items():
        pos = prog.positions.get(("machine", name))
        if pos:
            key = prefix + name
            idx.machines[key] = Symbol(key, "machine", pos[0], pos[1], _machine_detail(name, cfg), uri)


def _walk_imports(prog, base_path: Optional[Path], idx: SymbolIndex, seen: set, prefix: str = "") -> None:
    """Recursively pull event / guard / fragment declarations from imported files,
    tagging each with that file's uri (so go-to-definition lands there) and threading
    the alias prefix (`import "x.stm" as r` ⇒ its fragments become `r.Frag`)."""
    for path, alias, _pos in prog.imports:
        resolved = ((base_path / path) if base_path is not None else Path(path)).resolve()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        try:
            iprog = parse(resolved.read_text())
        except (OSError, DslError):
            continue
        child_prefix = prefix + (f"{alias}." if alias else "")
        _walk_imports(iprog, resolved.parent, idx, seen, child_prefix)  # nested first
        _collect_decls(iprog, idx, resolved.as_uri(), child_prefix)


def index(text: str, *, base_path: Optional[Path] = None, uri: Optional[str] = None) -> SymbolIndex:
    """Collect the document's declarations, following `import`s for cross-file
    events / guards / fragments (each tagged with its source file's uri). States are
    local to the document's machines. Tolerant: a parse error yields an empty index;
    a build error still yields the declaration symbols, just no state symbols."""
    idx = SymbolIndex()
    try:
        prog = parse(text)
    except DslError:
        return idx

    _walk_imports(prog, base_path, idx, set())  # imported declarations (uri = their file)
    _collect_decls(prog, idx, uri)  # local declarations override (uri = the open document)

    for machine in prog.machines:
        try:
            defn, positions = definition_with_positions(text, machine, base_path=base_path)
        except DslError:
            continue
        idx.definitions[machine] = defn  # retained for scope-aware state resolution
        for full_path, node in defn.index.items():
            pos = positions.get(full_path)
            if pos is None or not node.name:
                continue
            sym = Symbol(node.name, "state", pos[0], pos[1], _state_detail(node), uri)
            idx.states_by_path[full_path] = sym
            idx.states.setdefault(node.name, []).append(sym)
    return idx


_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*")


@dataclass(frozen=True)
class WordRef:
    """The identifier under the cursor: its text, 0-based span on its line, and the
    referenced `category` (None if the preceding keyword does not imply one)."""

    word: str
    line: int  # 0-based
    start: int  # 0-based column (inclusive)
    end: int  # 0-based column (exclusive)
    category: Optional[str]


def _category(line: str, start: int) -> Optional[str]:
    """Infer what an identifier starting at column `start` refers to, from the
    token just before it on the line (and a `|` continues an `on A | B` list)."""
    before = line[:start].rstrip()
    if before.endswith("|"):
        return "event"
    m = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*$", before)
    kw = m.group(1) if m else ""
    if kw in _EVENT_KW:
        return "event"
    if kw in _STATE_KW:
        return "state"
    if kw in _FRAGMENT_KW:
        return "fragment"
    if kw in _GUARD_KW:
        return "guard"
    if kw in _INVOKE_KW:
        return "invoke"
    return None


def category_at(text: str, line: int, character: int) -> Optional[str]:
    """The referenced category at a 0-based cursor, inferred from the preceding
    keyword on the line — works even when the cursor is not on an identifier yet
    (e.g. right after `on `), which is what completion needs."""
    rows = text.splitlines()
    if not (0 <= line < len(rows)):
        return None
    return _category(rows[line], character)


_IMPORT_RE = re.compile(r'^\s*import\s+"([^"]*)"(?:\s+as\s+(\w+))?')


def import_at(text: str, line: int, character: int) -> Optional[str]:
    """If the 0-based cursor sits on an `import`'s path string (quotes included) or
    its alias, return the imported relative path; else None. Lets the server turn a
    click on `import "review.stm" as jobs` into a jump to that file."""
    rows = text.splitlines()
    if not (0 <= line < len(rows)):
        return None
    row = rows[line]
    m = _IMPORT_RE.match(row)
    if not m:
        return None
    open_q = row.index('"')
    close_q = row.index('"', open_q + 1)
    if open_q <= character <= close_q:
        return m.group(1)
    if m.group(2) and m.start(2) <= character <= m.end(2):
        return m.group(1)
    return None


def word_at(text: str, line: int, character: int) -> Optional[WordRef]:
    """The identifier under a 0-based (line, character) cursor, or None. A quoted
    name (`"Place Order"`) resolves to its inner text."""
    rows = text.splitlines()
    if not (0 <= line < len(rows)):
        return None
    row = rows[line]

    # quoted name: if the cursor sits inside a "..." take the inner text as the word
    for q in re.finditer(r'"([^"]*)"', row):
        if q.start() < character < q.end():
            return WordRef(q.group(1), line, q.start() + 1, q.end() - 1, _category(row, q.start()))

    for m in _IDENT.finditer(row):
        if m.start() <= character <= m.end():
            word = m.group(0)
            return WordRef(word, line, m.start(), m.end(), _category(row, m.start()))
    return None


# scope-opening declarations: `machine`/`state`/`orthogonal`/`fragment`/`final NAME`
_OPEN_DECL = re.compile(r'\s*(machine|state|orthogonal|fragment|final)\s+("[^"]*"|\w+)')


def _classify_open(prefix: str) -> tuple[str, Optional[str]]:
    """Classify a `{` from the text before it on its line: the body of a
    machine/state/orthogonal/fragment/final declaration (a named scope) or any other
    block (`select`, `with`, `bind`, an event/enum body, inline `invoke` — not a
    state scope). Returns (kind, name) with kind in {machine, scope, other}."""
    m = _OPEN_DECL.match(prefix)
    if not m:
        return ("other", None)
    name = m.group(2).strip('"')
    kw = m.group(1)
    if kw == "machine":
        return ("machine", name)
    if kw == "fragment":
        return ("other", None)  # fragments are not part of a machine's Definition
    return ("scope", name)  # state / orthogonal / final


def scope_at(text: str, line: int, character: int) -> tuple[Optional[str], str]:
    """The enclosing (machine_name, scope_full_path) at a 0-based cursor — the
    state/orthogonal blocks the cursor sits inside (the machine root contributes the
    empty path, fragments are skipped). Used to resolve a state reference the way the
    engine does: relative to its scope."""
    rows = text.splitlines()
    stack: list[tuple[str, Optional[str]]] = []
    for li, row in enumerate(rows):
        if li > line:
            break
        upto = character if li == line else len(row)
        in_str = False
        i = 0
        while i < upto and i < len(row):
            c = row[i]
            if in_str:
                if c == "\\":
                    i += 2
                    continue
                if c == '"':
                    in_str = False
            elif c == '"':
                in_str = True
            elif c == "#" or (c == "/" and i + 1 < len(row) and row[i + 1] == "/"):
                break  # comment to end of line
            elif c == "{":
                stack.append(_classify_open(row[:i]))
            elif c == "}":
                if stack:
                    stack.pop()
            i += 1
    machine = next((n for k, n in stack if k == "machine"), None)
    scope_path = ".".join(n for k, n in stack if k == "scope" and n)
    return machine, scope_path


def resolve_state(idx: SymbolIndex, text: str, line: int, character: int, name: str) -> Optional[Symbol]:
    """Scope-aware resolution of a state reference `name` at a 0-based cursor: find
    the enclosing scope and resolve `name` from it the way the engine does
    (`resolve_relative` — descend then walk up ancestors). Falls back to None when
    the cursor is not inside a known machine (the caller then uses the first match)."""
    machine, scope_path = scope_at(text, line, character)
    defn = idx.definitions.get(machine) if machine else None
    if defn is None:
        return None
    scope_node = defn.index.get(scope_path) or defn.root
    node = resolve_relative(scope_node, name)
    if node is None:
        return None
    return idx.states_by_path.get(node.full_path)
