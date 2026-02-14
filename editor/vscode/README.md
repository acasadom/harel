# harel DSL — VSCode extension

Editor support for the `harel` statechart DSL (`.stm`):

- **Syntax highlighting** (TextMate grammar) — works standalone, no server.
- **Live diagnostics** — a Python language server reuses the engine's own
  `parse()` + `validate()`, so parse errors, unresolved targets, unbound handlers
  and validation findings are squiggled inline with the exact source position.
- **Hover** — info on a state (kind, children, hooks, outcome), event (fields) or
  guard under the cursor.
- **Go-to-definition** — jump from a state / event / guard / fragment reference to
  its declaration.
- **Completion** — context-aware names (events after `on`, states after `to` /
  `from` / `initial`, fragments after `use`) plus the language keywords.
- **Live statechart preview** — *Open Statechart Preview* (editor title bar / the
  command palette on a `.stm`) opens a side panel that renders the machine as a
  Mermaid `stateDiagram-v2` and re-renders as you type, with pan + zoom. Read-only
  (a live view, not a graphical editor). A **picker** in the toolbar switches between
  the document's machines, fragments and imported `invoke` submachines (a fragment is
  drawn with placeholder args, flagged by a note). The diagram comes from the server
  (`harel/render`), so it needs the language server enabled; it is drawn by the
  vendored `media/mermaid.min.js` (no Java / render server, unlike PlantUML).

## Layout

```
editor/vscode/
  package.json                 # language + grammar + LSP client contributions
  language-configuration.json  # comments, brackets, auto-close
  syntaxes/stm.tmLanguage.json # highlighting
  src/extension.js             # LSP client + preview panel
  media/mermaid.min.js         # vendored Mermaid (drawn in the preview webview)
  media/preview.js             # preview webview script (receives renders, draws)
```

The server lives in the Python package: `harel/lsp/` (`python -m harel.lsp`,
or the `harel-lsp` console script). Install it with the `lsp` extra:

```bash
uv sync --extra lsp        # or: pip install "harel[lsp]"
```

## Run it (development)

Install the client dependency (`vscode-languageclient`) with any JS package
manager — `npm install`, or **`bun install`** (no Node needed; bun is enough):

```bash
cd editor/vscode
bun install                # or: npm install
```

Then either:

- **F5** — open `editor/vscode/` in VS Code and press F5 → "Run Extension"
  (Extension Development Host). Open a `.stm` in the dev host.
- **Unpacked install** — `make vscode-install` from the repo root (runs `bun
  install` and copies this folder, with `node_modules/` + `media/`, into
  `~/.vscode/extensions/harel-<version>/`); then reload VS Code. No npm/vsce,
  no packaging step.

Highlighting is immediate; diagnostics / hover / goto / completion appear once the
server starts. The server interpreter is auto-detected from a `.venv/` in the
workspace folder (so opening the harel repo Just Works); override with the
settings below.

> Packaging a `.vsix` with `@vscode/vsce` requires `npm` on PATH (vsce shells out
> to it); the F5 / unpacked routes above do not.

## Settings

- `harel.pythonPath` (default `python`) — interpreter that has `harel[lsp]`.
  When left at the default, a `.venv/` in the workspace folder is auto-detected;
  otherwise point it at your venv, e.g. `/path/to/.venv/bin/python`.
- `harel.serverCommand` — override the whole command, e.g. `["uv", "run",
  "harel-lsp"]` (used verbatim when non-empty).
- `harel.enableLanguageServer` (default `true`) — set `false` for highlighting
  only.

## Packaging (optional)

```bash
npm install -g @vscode/vsce
vsce package               # produces harel-0.1.0.vsix
```

## Mermaid (preview)

`media/mermaid.min.js` is vendored (committed) so the preview works offline with no
`node_modules` at runtime, the same way highlighting does. Refresh it with:

```bash
bun add mermaid                              # records the version in package.json
cp node_modules/mermaid/dist/mermaid.min.js media/mermaid.min.js
```

## Scope

Diagnostics, hover, go-to-definition, completion and the live preview. Imported events / guards /
fragments are resolved across files (go-to-definition jumps into the imported
`.stm`); states are resolved within the open document. Completion offers declared
symbols when the document currently parses; otherwise it falls back to the language
keywords. Go-to-definition is scope-aware: a state name that repeats across scopes
resolves to the declaration the engine would pick from the cursor's scope.
