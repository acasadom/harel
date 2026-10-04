# Minimal example

> How it works — the machine, how it is built, sequence diagrams, and why it runs the way it
> does: [the minimal page](https://acasadom.github.io/harel/examples/minimal.html) of the docs.

The smallest harel machine that runs — copy it to start your own. (For a full,
production-shaped example with hierarchy, selectors and retry, see
[`../place_order/`](../place_order/).)

```bash
uv run python -m examples.minimal.run        # drive it headless
harel run examples/minimal/approval.stm -e Submit -e Approve   # or via the CLI
```

Or scaffold a fresh one anywhere:

```bash
harel new mymachine.stm
```
