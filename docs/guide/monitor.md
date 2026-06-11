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

**Detail** — opened with `enter`. The left column shows the **statechart tree** with the **active
state highlighted** (ancestors bolded; orthogonal regions and `invoke` children annotated with their
join outcome), and a collapsible **DSL source** panel underneath (the machine's `.stm`, folded by
default — needs `--definitions-dir`). The right column is a status header, a row of control buttons,
and a navigable **execution timeline** over a per-step **detail**.

Navigate the timeline (`↑/↓`) and each step shows its **event in, transition, guards, actions, and
context before → after**; the step's target state is marked in the tree (a `◀` marker) so you can
see both the live state and the one you're inspecting. Control-plane actions are both **keys and
buttons** (the buttons enable/disable per status):

| key / button | action | |
|-----|--------|--|
| `s` Suspend | suspend | immediate (RUNNING → SUSPENDED) |
| `R` Resume | resume | immediate (SUSPENDED → RUNNING) |
| `c` Cancel | cancel | confirm; modelled if the machine has an `on Cancel`, else forceful; disabled when the Definition is unresolved |
| `t` Terminate | terminate | confirm; forceful (→ CANCELLED) |
| `escape`/`h` | back | |

The destructive actions (`c`/`t`) ask for confirmation. All of these go through the
[control plane](control-plane.md), which CASes the record so the change lands at the next event
boundary.

> **The timeline is opt-in.** Recording it is **off by default** (the engine keeps a state snapshot,
> not an event log, so the hot path pays nothing). Enable it with **`STM_TRACE=1`** (or
> `DurableRunner(..., trace=True)` / `DistributedRunner(..., trace=True)`): the Driver then records one
> step per event — event in, transition from→to, the actions run, and the resulting context — **in the
> same `commit` transaction** as the state advance (no extra round-trip, and `load` is unaffected). Kept
> as a ring of the last `STM_TRACE_MAX` steps (default 200). Recorded by **every store backend** in its
> commit transaction / atomic write (SQL via an in-txn insert, Redis via a ring list in the MULTI, Mongo
> via a `$push/$slice` array, DynamoDB via a Put+Delete in the `TransactWriteItems`). Without a trace the
> timeline shows a placeholder; the tree, source, status, control plane and pending-work panels all work
> regardless.

## Theming

The palette comes from a built-in **Textual theme** (`nord` by default) — pick another with
`--theme <name>` / `STM_TUI_THEME`, or preview them live in-app with **`Ctrl+P` → "theme"**
(`gruvbox`, `tokyo-night`, `dracula`, `textual-light`, …).

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
