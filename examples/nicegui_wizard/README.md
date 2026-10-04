# Durable wizard — example (harel + NiceGUI)

A multi-step onboarding wizard whose **state lives on the server, in a durable
store** — so it survives a browser reload *and* a server restart and resumes on the
exact step the user had reached. The whole UI flow is one statechart (`wizard.stm`);
each button click is just an awaited `runner.process(...)`.

This is the niche where harel's durability stops being overkill for a UI: an
in-browser [XState](https://stately.ai/docs/xstate) loses its state on reload, and a
plain in-memory FSM loses it on restart — a **durable, server-side statechart** keeps it.

```bash
pip install -r examples/nicegui_wizard/requirements.txt
python -m examples.nicegui_wizard.app           # open http://localhost:8080
```

## The "it can't lose your progress" demo

1. Fill in **Account** (email) → **Next**, fill **Profile** (name) → **Next**.
2. **Reload the page** (F5) — you are still on **Verify**, data intact.
3. **Stop the server** (Ctrl-C) and **start it again** — reload — *still* on **Verify**.
   The Execution was checkpointed to `wizard.db`; the step is keyed to your browser
   session (`app.storage.user`).
4. Watch the **statechart panel** beside the form: it's `wizard.stm` rendered to
   Mermaid by `harel.viz.mermaid`.

## How it maps

- **`wizard.stm`** — the machine: `Account → Profile → Verify → Done`, with `Back`.
  **Guards** (`account_ok`/`profile_ok`) read the field on the `Next` event, so the
  machine refuses to advance with an empty field; **`set`** keeps the typed field in a
  typed `context { ... }` (it survives Back/Next and the restart); and `Verify` only
  completes `on Verified where event.code == context.code` — the typed code must match
  the one the machine sent.
- **`actions.py`** — `send_code` simulates emailing a verification code.
- **`app.py`** — the glue (~40 lines): load-or-create the Execution for this session
  from a durable `AsyncSqliteStore`, render the step named by `exe.active_path`, and turn
  each click into an `Event`.

## Why the async API

NiceGUI runs on an asyncio event loop, and calls its handlers on that loop. So the app uses
harel's async API — `AsyncDurableRunner` over an `AsyncSqliteStore`, awaited from `async`
handlers: while one click waits on the store, the loop keeps serving every other browser. The
sync `DurableRunner` would block that loop for the whole call, and harel refuses a sync call
from inside a running loop for that reason (see [execution models](../../docs/guide/execution.md)).

## Honest notes

- **What the statechart owns:** the step sequence and its rules — the advance guards
  and the code match. The app owns the inputs: it sends what was typed and shows where
  the machine is.
- The Mermaid panel shows the **static** diagram; highlighting the active node live is
  a natural next enhancement.
- `wizard.db` and NiceGUI's `.nicegui/` session store are created on first run; delete
  them to wipe all in-progress wizards.
