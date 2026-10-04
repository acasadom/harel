# minimal — the smallest machine

## What it does

A two-step approval: a draft is submitted for review, and the review approves it, rejects it,
or sends it back for changes. The script starts one approval and feeds it `Submit` and
`Approve`, printing where the machine is after each event and the verdict it ends with:

```text
(start)  -> Draft
Submit   -> Review
Approve  -> Approved
status: DONE  outcome: success
```

It is the file to copy when you start a machine of your own (or run `harel new`).

## The machine

`approval.stm` declares the events, the states, the two terminals with their verdicts
(`final Approved success`, `final Rejected rejected`) and the transitions — no actions, no
context:

<!-- machine: examples/minimal/approval.stm approval -->
```{mermaid}
stateDiagram-v2
[*] --> Draft
Approved : outcome: success
Rejected : outcome: rejected
Draft --> Review : Submit
Review --> Approved : Approve
Review --> Rejected : Reject
Review --> Draft : RequestChanges
Approved --> [*]
Rejected --> [*]
```

## How it is built

`run.py` compiles the machine (`definition_from_dsl_file(..., validate=True)` runs the static
checks first), builds a `DurableRunner` over an in-memory `DictStore`, and drives it:

```text
runner = DurableRunner(DictStore(), {defn.id: defn})
exe = runner.create(defn.id)
for kind in ("Submit", "Approve"):
    exe = runner.process(exe.id, Event(kind=kind))
```

`create` starts the execution and returns it as committed; each `process` loads it, runs the
engine for one event, commits, and returns it.

```{mermaid}
sequenceDiagram
  participant S as run.py
  participant R as DurableRunner
  participant D as DictStore
  S->>R: create("approval")
  R->>D: commit(exe v1: Draft)
  R-->>S: exe (Draft)
  S->>R: process(id, Submit)
  R->>D: load(id)
  R->>D: commit(exe v2: Review)
  R-->>S: exe (Review)
  S->>R: process(id, Approve)
  R->>D: load(id)
  R->>D: commit(exe v3: Approved, DONE)
  R-->>S: exe (Approved, success)
```

## How it runs

- **Semantics: synchronous.** The script needs each result before it sends the next event, so
  it uses `DurableRunner`: `process` returns the execution once the event has been processed.
- **Execution model: background, the default.** A plain script has no thread or connection to
  stay on, so it doesn't choose: the runner works on the shared background event loop, and each
  call blocks until it is done.

Swap `DictStore()` for `SqliteStore("approvals.db")` and the approval survives a restart: run
`runner.process` with the same id later, from another process, and it continues where it was.

## Run it

```text
uv run python -m examples.minimal.run
harel run examples/minimal/approval.stm approval -e Submit -e Approve   # the same, from the CLI
```
