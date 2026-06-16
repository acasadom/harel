# Durable wizard — example (harel + NiceGUI)

A multi-step onboarding wizard whose **state lives on the server, in a durable
store** — so it survives a browser reload *and* a server restart and resumes on the
exact step the user had reached. The whole UI flow is one statechart (`wizard.stm`);
each button click is just `runner.process(...)`.

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

- **`wizard.stm`** — the machine: `Account → Profile → Verify → Done`, with `Back`,
  and **guards** (`account_ok`/`profile_ok`) that read the field on the `Next` event
  so the machine refuses to advance with an empty field (illegal states unreachable).
- **`actions.py`** — `save` persists the typed field into the context (survives
  Back/Next and the restart); `send_code` simulates emailing a verification code.
- **`app.py`** — the glue (~40 lines): load-or-create the Execution for this session
  from a durable `SqliteStore`, render the step named by `exe.active_path`, and turn
  each click into an `Event`.

## Honest notes

- **What the statechart owns:** the *step sequence* and the advance guards. Field
  validation (the verification code match) stays in the UI — the model owns the flow,
  the app owns the inputs.
- The Mermaid panel shows the **static** diagram; highlighting the active node live is
  a natural next enhancement.
- `wizard.db` and NiceGUI's `.nicegui/` session store are created on first run; delete
  them to wipe all in-progress wizards.
