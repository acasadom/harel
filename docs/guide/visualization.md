# Visualization & tooling

A statechart is worth drawing, and worth editing with help. harel renders any machine to a
diagram and ships a language server plus a VSCode extension with a live preview.

## Render to a diagram

The same `Definition` you run can be rendered — to PlantUML or to Mermaid — by walking the node
tree. No diagram server, no Java:

```python
from harel import definition_from_dsl, render          # render = PlantUML
from harel.viz import mermaid                            # the browser-friendly sibling

SOURCE = """
event Finish {}

machine order {
  initial Cart
  state Cart {}
  final Done success {}
  from Cart to Done on Finish
}
"""

defn = definition_from_dsl(SOURCE, "order")
print(render(defn))
print("---")
print(mermaid.render(defn))
```

```text
[*] --> Cart
Cart --> Done: Finish
Done --> [*]
---
stateDiagram-v2
[*] --> Cart
Done : outcome: success
Cart --> Done : Finish
Done --> [*]
```

PlantUML is good for docs and review; Mermaid (`stateDiagram-v2`) renders directly in a browser
— which is what the live preview below uses. Both walk the tree by reference and handle the full
language: composites carry their hooks, orthogonal regions become nested concurrent blocks.

## The language server

`harel.lsp` is a DSL language server (install the `lsp` extra, run `python -m harel.lsp`
or the `harel-lsp` script). The pure analysis core, `lsp.analyze(text)`, parses and validates
into a list of `Diagnostic`s — the same `validate` findings from [step 14](../tutorial/14-validation),
mapped back to their source location. On top of that the server provides:

- **Diagnostics** — parse errors (with a caret snippet) and validation findings as you type.
- **Hover, go-to-definition, completion** — for events, guards, fragments, and `invoke` targets,
  resolving **across `import`s** (go-to-definition on `invoke ns.review` jumps into the imported
  file; go-to-definition on an import path opens that file).

## The VSCode extension & live preview

`editor/vscode/` is a VSCode extension: TextMate syntax highlighting, an LSP client, and a
**live statechart preview**. *Open Statechart Preview* renders the active `.stm` as a Mermaid
diagram and re-renders as you type, drawn by a vendored `mermaid.min.js` (offline, no CDN). The
preview pans and zooms, offers a picker across every machine/fragment/imported submachine in the
document, and keeps the last valid diagram on screen while you're mid-edit. It even renders a
fragments-only file by wrapping the fragment in a synthetic preview machine.

## The formatter

`harel-fmt` (or `python -m harel.fmt`) is a brace-aware reindenter — canonical 2-space
indentation, comment-preserving, idempotent. `make fmt-stm` formats every `.stm` in place;
`make fmt-stm-check` (folded into `make lint`) verifies formatting in CI.
