# Minimal example

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
