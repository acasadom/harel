# Monitoring TUI (`harel monitor`)

A terminal UI to watch executions run — "k9s for statecharts". List the executions in a store,
drill into one to see its **statechart with the active state highlighted** plus its data
(status, machine version, error, context, history, pending timers and events, children), and
drive the **control plane** (suspend/resume/cancel/terminate/redrive) by keyboard. It polls the store, so it works against any backend
and any running worker fleet — it's a read/observe client, not part of the engine.

```bash
pip install "harel[tui]"        # textual
harel monitor                   # uses HAREL_STORE_BACKEND etc., like the worker
harel monitor --definitions-dir ./machines   # resolve the statechart diagram
```

## Connecting

The monitor builds its store from the **same environment variables as the worker**
(`HAREL_STORE_BACKEND` + the backend's URL/DSN/endpoint — see [Distribution](distribution.md)). Point
it at the store your workers share and it lists what's there. No store of its own.

To draw the **statechart tree** it also needs the machine's `Definition` — the **version** each
execution runs on (`definition_version`). It finds it in the `.stm` files you give it:

```bash
harel monitor --definitions-dir ./machines   # or $HAREL_DEFINITIONS_DIR
```

and, for a version those files don't have, among the versions **published** in the store (see
[changing a machine](durability.md#changing-a-machine-with-executions-in-flight)) — so an
execution still on version 1 is drawn with version 1, and its DSL source shown, after the files
moved on to version 2. Nothing it builds from the store is run: the monitor runs no actions.

Without either the monitor still runs **data-only**: every panel works (status, context, timers,
history), but the tree shows a placeholder, and `cancel` and `redrive` are disabled — they need the
Definition (to decide cooperative-vs-forceful, and to know the states); `terminate`/`suspend`/`resume`
still work.

## The two screens

**List** — the executions, a page at a time, auto-refreshing: id, machine and version (`order v2`),
status, outcome, active state, and how long ago it was last written. The **store** filters and
pages it, so it reaches every execution however many there are:

| key | |
|-----|--|
| `f` | cycle the status filter: any → live (pending, running, suspended, cancelling) → failed → done → cancelled |
| `a` | show the children too (orthogonal regions, fan-out instances, invoked machines); by default only the roots of their trees |
| `n` / `b` | next / previous page |
| `/` | narrow the page shown by a substring of id, machine or status (`escape` clears it) |
| `w` | the queues and the workers (below) |
| `r` / `p` | refresh now / pause or resume the auto-refresh |
| `enter` | open the selected execution |

**Detail** — opened with `enter`. On the left, the **statechart tree** with the **active state
highlighted** (ancestors bolded; orthogonal regions and `invoke` children annotated with their join
outcome) and the machine's **DSL source**, folded — the `.stm` of the version the execution runs,
from `--definitions-dir`, or from the store when it was published.

On the right, a header — status, id, the **machine version** and its source digest, the root
machine of its tree when it is a child of another's, its parent, outcome, and when it was created
and last written — then its **error**, whole, when it has one (a dead letter's reason, a
`DefinitionChanged`), the control buttons, and four tabs:

- **Timeline** — one step per event, when the runners record one (see below): navigate it with
  `↑/↓` and each step shows its **event in, transition, guards, actions, what a `set` wrote, and
  context before → after**; the step's target state is marked in the tree (`◀`).
- **Context** — the execution's context and its history (the last active child of each composite),
  trace or no trace.
- **Pending** — what is waiting for it: armed timers (and when they fire), events queued for it
  but not delivered yet, children it is about to spawn.
- **Children** — its regions and invoked machines, with their join state; `enter` opens one.
  `u` goes up to the parent from a child.

Control-plane actions are both **keys and buttons**, each enabled only where it applies:

| key / button | action | |
|-----|--------|--|
| `s` Suspend | suspend | a running execution, immediately (RUNNING → SUSPENDED) |
| `R` Resume | resume | a suspended one, immediately (SUSPENDED → RUNNING) |
| `c` Cancel | cancel | a live one; confirm; modelled if the machine has an `on Cancel`, else forceful; needs the Definition |
| `t` Terminate | terminate | any unfinished one, a failed one included; confirm; forceful (→ CANCELLED) |
| `d` Redrive | redrive | a failed one, at the **leaf state selected in the tree**; confirm; needs the Definition — see [redrive](control-plane.md#redrive--repairing-a-dead-letter) |
| `u` | parent | open the parent of a child |
| `escape`/`h` | back | |

All of them go through the [control plane](control-plane.md), which CASes the record so the change
lands at the next event boundary; one it refuses (a redrive target it can't resume from, say)
shows its reason in a toast.

> **The timeline is opt-in.** Recording it is **off by default** (the engine keeps a state snapshot,
> not an event log, so the hot path pays nothing). Enable it with **`HAREL_TRACE=1`** (or
> `DurableRunner(..., trace=True)` / `DistributedRunner(..., trace=True)`): the runners then record
> one step per event — event in, transition from→to, the actions run, and the resulting context —
> **in the same `commit` transaction** as the state advance (no extra round-trip, and `load` is
> unaffected). Kept as a ring of the last `HAREL_TRACE_MAX` steps (default 200), by every store
> backend. Without it the Timeline tab says so; everything else works regardless.

## Queues and workers

`w` from the list opens the work queued and who is there to do it:

- **Queued work, by machine** — for each machine (the root machine of its executions' trees), how
  many messages wait, for how many executions, how many of those a worker holds (leased now, or
  parked: a suspended execution's, or one retried after a failure), how long the oldest has
  waited, and which **live workers** claim it. A machine with work waiting that no live worker
  claims is marked, and named at the top — the case [dedicated workers](distribution.md#dedicating-workers-to-machines)
  can leave when every worker is given `machines`. A machine a live worker is dedicated to shows
  even with nothing queued, so its capacity is visible.
- **Workers** — every worker's heartbeat: the machines it claims (`all` for one given none), when
  it started and when it was last seen. A worker records one when it starts and every `heartbeat`
  seconds (10 by default), and removes it when it stops; one not seen for three beats is shown
  **gone**. A worker that crashed never removed its own: `x` forgets the selected one, once gone.

Reading the backlog walks every group with work queued, so the screen refreshes on its own,
slower interval — 10 seconds by default (`--queue-interval`, `HAREL_TUI_QUEUE_INTERVAL_MS`) — and
on `r`. With 20,000 groups queued it takes about 20–30 ms on SQLite and Postgres and 90 ms on
Redis, which reads it in chunks of 1,000 groups so no single call holds the server more than a
few milliseconds.

The backlog comes from the **transport** the workers use, built from the same `HAREL_TRANSPORT_*`
environment; without one the screen says so and shows the workers only. SQS counts its whole
queue, approximately, not by machine — the screen says that too. A message queued before the
transport kept publish times (`SCHEMA_VERSION` 6) has no age.

## Theming

The palette comes from a built-in **Textual theme** (`nord` by default) — pick another with
`--theme <name>` / `HAREL_TUI_THEME`, or preview them live in-app with **`Ctrl+P` → "theme"**
(`gruvbox`, `tokyo-night`, `dracula`, `textual-light`, …).

## How it refreshes

There is no event stream, so the monitor **polls** the store on a timer (default 1s, `--interval`
seconds or `HAREL_TUI_INTERVAL_MS`). Every store read runs on a worker thread, so a slow networked
store never freezes the UI; a failed action surfaces as a toast and the next poll reconciles. On a
busy store, raise `--interval` or pause with `p`.

## Notes

- The list is a lightweight projection (`ExecutionStore.list_executions` → `ExecutionSummary`),
  filtered and paged by the store; the full record is loaded only when you open an execution. On
  the durable backends the status filter is applied as the list is scanned (status isn't a
  broken-out column) — fine for operator volumes. The list is ordered by id.
- The detail reads one execution: its record, its trace, and what is pending for it
  (`ExecutionStore.pending_work`), each by the execution's id — so a refresh costs the same however
  large the store.
- The pure model (`harel.tui.model`/`tree`/`summary`) has no textual dependency and is unit-tested
  on its own; the textual UI is tested through Textual's Pilot.
