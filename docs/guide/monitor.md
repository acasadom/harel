# Monitoring TUI (`harel monitor`)

A terminal UI to watch executions run — "k9s for statecharts". List the executions in a store,
drill into one to see its **statechart with the active state highlighted** plus its data
(status, outcome, context, pending timers, history), and drive the **control plane**
(suspend/resume/cancel/terminate) by keyboard. It polls the store, so it works against any backend
and any running worker fleet — it's a read/observe client, not part of the engine.

```bash
pip install "harel[tui]"        # textual
harel monitor                   # uses STM_STORE_BACKEND etc., like the worker
harel monitor --definitions-dir ./machines   # resolve the statechart diagram
```

## Connecting

The monitor builds its store from the **same environment variables as the worker**
(`STM_STORE_BACKEND` + the backend's URL/DSN/endpoint — see [Distribution](distribution.md)). Point
it at the store your workers share and it lists what's there. No store of its own.

To draw the **statechart tree** it also needs the machine's `Definition`, which the store doesn't
persist — only `definition_id`. Give it the `.stm` files:

```bash
harel monitor --definitions-dir ./machines   # or $STM_DEFINITIONS_DIR
```

Without them the monitor still runs **data-only**: every panel works (status, context, timers,
history), but the tree shows a placeholder and `cancel` is disabled (it needs the Definition to
decide cooperative-vs-forceful — `terminate`/`suspend`/`resume` still work).

## The two screens

**List** — a table of executions (id, definition, status, outcome, active state, version),
auto-refreshing. Keys: `enter` opens the selected execution, `/` filters by a free substring
(id/definition/status), `r` forces a refresh, `p` pauses/resumes auto-refresh, `q` quits.

**Detail** — the statechart tree on the left with the **active state highlighted** (and its
ancestors bolded; orthogonal regions and `invoke` children annotated with their join outcome), and
data panels on the right (status/outcome/error, context, pending timers/events/spawns, history).
Control-plane keys:

| key | action | |
|-----|--------|--|
| `s` | suspend | immediate (RUNNING → SUSPENDED) |
| `R` | resume | immediate (SUSPENDED → RUNNING) |
| `c` | cancel | confirm; modelled if the machine has an `on Cancel`, else forceful |
| `t` | terminate | confirm; forceful (→ CANCELLED) |
| `escape`/`h` | back | |

The destructive actions (`c`/`t`) ask for confirmation. All of these go through the
[control plane](control-plane.md), which CASes the record so the change lands at the next event
boundary.

## How it refreshes

There is no event stream, so the monitor **polls** the store on a timer (default 1s, `--interval`
seconds or `STM_TUI_INTERVAL_MS`). Every store read runs on a worker thread, so a slow networked
store never freezes the UI; a failed action surfaces as a toast and the next poll reconciles. On a
busy store, raise `--interval` or pause with `p`.

## Notes

- Listing is a lightweight projection (`ExecutionStore.list_executions` → `ExecutionSummary`); the
  full record is loaded only when you open an execution. On the durable backends the status filter
  is applied as the list is scanned (status isn't a broken-out column) — fine for operator volumes.
- The pure model (`harel.tui.model`/`tree`/`summary`) has no textual dependency and is unit-tested
  on its own; the textual UI is tested through Textual's Pilot.
