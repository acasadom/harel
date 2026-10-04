# Command-line interface

Installing harel puts a single `harel` command on your `PATH` that wraps the static tooling,
an in-memory `run`, the formatter, and the language server.

```text
harel new      FILE [NAME] [--force]        # scaffold a starter .stm (validates + runs as-is)
harel validate FILE [NAME]                 # parse + validate; exit 1 on errors
harel render   FILE [NAME] [--mermaid]      # PlantUML (default) or Mermaid
harel list     FILE                         # machines / fragments / events in a file
harel run      FILE [NAME] [-e KIND[:JSON]] # drive a machine with events (in-memory)
harel fmt      FILES... [--check|--diff]    # format .stm files
harel lsp                                   # start the DSL language server (stdio)
harel monitor  [--definitions-dir DIR]      # the monitoring TUI (needs the `tui` extra) — see Monitor
harel purge    --older-than AGE [...]       # delete finished executions older than AGE (real store)
harel --version
```

`NAME` selects the machine when a file declares more than one.

## Starting from scratch

`harel new` writes a small, commented machine that **validates and runs with no setup** —
zero to a working state machine in one command:

```text
$ harel new approval.stm
created approval.stm  (machine approval)
next:
  harel validate approval.stm
  harel run      approval.stm -e Submit -e Approve

$ harel run approval.stm -e Submit -e Approve
(start)              -> Draft
Submit               -> Review
Approve              -> Approved
status: DONE  outcome: success
```

The machine name defaults to the file name (sanitised to a valid identifier); pass `NAME`
to override, and `--force` to overwrite an existing file.

## Examples

Validate a machine and render it:

```text
$ harel validate examples/place_order/order.stm order
order: ok

$ harel render examples/place_order/order.stm order --mermaid
stateDiagram-v2
[*] --> Cart
...
```

Drive a machine with a sequence of events (each `-e` is one event; attach data as
`KIND:'{...}'` for guarded transitions):

```text
$ harel run examples/place_order/order.stm order \
    -e PlaceOrder -e PaymentAuthorized -e Picked -e Packed -e Dispatched -e Delivered
(start)              -> Cart
PlaceOrder           -> AwaitingPayment
PaymentAuthorized    -> Fulfilling.Picking
...
status: DONE  outcome: success
```

`run` resolves a machine's action modules from the working directory (for package-qualified
paths like `pkg.mod.fn`, run it from your project root) and from the `.stm` file's own
directory. Seed the initial context with `--seed '{"items": [...]}'`, and add `--validate` to
check the machine before running.

## Purging finished executions

`harel purge` permanently deletes the finished execution trees (root, regions, invokes and all
their store rows) whose root finished at least `AGE` ago — `30d`, `12h`, `90m`, `45s`, `2w`. It
works on the **real store**, configured from the same `HAREL_STORE_*` environment as the worker and
the monitor, so run it as a scheduled job alongside the workers rather than inside them:

```text
$ harel purge --older-than 30d --dry-run          # report only
would purge: 1284
$ harel purge --older-than 30d --archive /backups/harel.jsonl
purged: 1284
```

- `--archive PATH` appends each tree to a JSONL file (one fsynced line per tree) before deleting it.
- `--status done` / `--status cancelled` narrows the candidates (default: both). `FAILED` dead
  letters are never purged — `terminate` one to abandon it first.
- `--limit N` caps one run; `-v` lists the purged root ids.
- Executions written before `finished_at` was recorded carry none, and are skipped (and counted)
  unless `--include-undated`.
- A tree that can't be purged (a member still live, or changed concurrently) is reported on
  stderr and skipped; the exit code is 1 if any was.

See [purge](control-plane.md#purge) for the semantics.

`fmt` and `lsp` are passthroughs: `harel fmt --check **/*.stm` and `harel lsp` behave exactly
like the standalone `harel-fmt` / `harel-lsp` entry points.

## Verified

The commands behave as shown — exercised in CI:

```python
from harel.cli import main

assert main(["list", "test/data/order.stm"]) == 0
assert main(["validate", "test/data/order.stm", "order"]) == 0
assert main(["render", "test/data/order.stm", "order", "--mermaid"]) == 0
```
