# nicegui_wizard — a form that survives reloads and restarts

## What it does

A multi-step onboarding wizard in the browser — account, profile, a verification code — whose
state lives on the server, in a durable store, keyed to the browser session. Reload the page,
or stop and restart the server, and the user is back on the step they had reached, with what
they had typed. The machine refuses to advance on an empty field, and only completes when the
typed code matches the one it sent.

## The machine

<!-- machine: examples/nicegui_wizard/wizard.stm wizard -->
```{mermaid}
stateDiagram-v2
[*] --> Account
state "Verify" as Verify
Verify : on enter#58; send_code
state "Done" as Done
Done : outcome#58; success
Account --> Profile : Next<br/>[email != '']<br/>/ context.email = event.email
Profile --> Account : Back
Profile --> Verify : Next<br/>[full_name != '']<br/>/ context.full_name = event.full_name
Verify --> Profile : Back
Verify --> Done : Verified<br/>[code == context.code]
Done --> [*]
```

The machine owns the flow and its rules: each `Next` carries the field of the step it leaves,
a named guard (`account_ok`, `profile_ok`) refuses an empty one, and `set` keeps it in the typed
context. Entering `Verify` runs `send_code`, which puts a code in the context; `Verified` only
completes `where event.code == context.code`.

## How it is built

- `wizard.stm` — the machine.
- `actions.py` — `send_code`, which simulates emailing a code (the demo shows it on the page).
- `app.py` — the NiceGUI glue, about 40 lines:
  - `runner()` builds an `AsyncDurableRunner` over an `AsyncSqliteStore` (`wizard.db`) on first
    use, so the store opens on NiceGUI's own event loop;
  - `_execution()` loads this session's execution (its id is in `app.storage.user`) or creates
    one;
  - `wizard_ui()`, a refreshable, renders the step named by `exe.active_path`;
  - each button's handler turns the click into an `Event`, awaits `runner.process(...)`, and
    awaits the refresh.

A click:

```{mermaid}
sequenceDiagram
  actor B as Browser
  participant L as NiceGUI event loop
  participant R as AsyncDurableRunner
  participant S as AsyncSqliteStore
  participant P as thread pool
  B->>L: click Next (email)
  L->>R: await process(id, Next)
  R->>S: await load(id)
  Note over L: while the store works, the loop serves other browsers
  R->>R: Account to Profile, set context.email
  R->>S: await commit(exe v+1)
  R-->>L: exe (Profile)
  L->>B: re-render the Profile step
  B->>L: click Next (full name)
  L->>R: await process(id, Next)
  R->>P: send_code (a sync action)
  P-->>R: returns
  R->>S: await commit(exe: Verify, code in the context)
  L->>B: re-render the Verify step
```

## How it runs

- **Semantics: synchronous** — `AsyncDurableRunner`: a click must show the step it led to, so
  the handler waits for the event to be processed before it re-renders.
- **Execution model: coroutines.** NiceGUI calls its page functions and handlers on its asyncio
  event loop, so the app uses harel's async API, awaited from `async def` handlers. Each store
  call is awaited, and while one session's click waits on it, the loop serves every other
  session; the sync `send_code` runs in a thread pool, so it doesn't stall the loop either. A
  sync `DurableRunner` would block the loop for the whole call — and harel refuses a sync call
  from inside a running loop for that reason.
- **Durability**: every click is a committed checkpoint in `wizard.db`, which is why a restart
  loses nothing; delete it (and NiceGUI's `.nicegui/`) to start over.

## Run it

```text
pip install -r examples/nicegui_wizard/requirements.txt
python -m examples.nicegui_wizard.app           # open http://localhost:8080
```
