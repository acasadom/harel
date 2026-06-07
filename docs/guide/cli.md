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
