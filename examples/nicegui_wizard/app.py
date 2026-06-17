"""A durable onboarding wizard — harel + NiceGUI, state on the server.

    pip install -r examples/nicegui_wizard/requirements.txt
    python -m examples.nicegui_wizard.app

The wizard's state lives in a *durable* `SqliteStore` (`wizard.db`), keyed to the
browser session (`app.storage.user`). So it survives a browser reload *and* a server
restart: stop the process, start it again, refresh — you are back on the exact step
you had reached, with the data you had typed. The whole UI flow is one `.stm`
statechart (see `wizard.stm`); each button click is just `runner.process(...)`.

The glue is small on purpose: load-or-create the Execution for this session, render
the step named by `exe.active_path`, and turn each click into an `Event`.
"""

from pathlib import Path

from nicegui import app, ui

from harel import DurableRunner, Event, SqliteStore, definition_from_dsl_file
from harel.viz import mermaid

WIZARD_STM = Path(__file__).parent / "wizard.stm"
DB_PATH = Path(__file__).parent / "wizard.db"

defn = definition_from_dsl_file(WIZARD_STM, "wizard")
store = SqliteStore(DB_PATH)  # durable: survives a process restart
runner = DurableRunner(store, {defn.id: defn})


def _simple_diagram(definition) -> str:
    """A minimal `stateDiagram-v2` (states + event-kind labels only) that older bundled
    mermaid.js builds render reliably. `harel.viz.mermaid.render` emits richer labels
    (hook descriptions, `<br/>` guard predicates) that need a recent mermaid; NiceGUI
    ships an older one, so we strip to the lowest common denominator for the demo."""
    lines = []
    for line in mermaid.render(definition).splitlines():
        if "-->" not in line and " : " in line:
            continue  # drop "State : on enter: ..." description lines
        lines.append(line.split("<br/>")[0].rstrip())  # drop the "<br/>[guard]" suffix
    return "\n".join(lines)


DIAGRAM = _simple_diagram(defn)  # the statechart, drawn beside the form


def _execution():
    """Load this browser session's wizard, or start a fresh one."""
    exe_id = app.storage.user.get("exe_id")
    exe = store.load(exe_id) if exe_id else None
    if exe is None:
        exe = runner.create(defn.id)
        app.storage.user["exe_id"] = exe.id
    return exe


def _advance(exe_id: str, kind: str, **data) -> None:
    """Send a domain event into the Execution, then redraw."""
    runner.process(exe_id, Event(kind=kind, data=data))
    wizard_ui.refresh()


def _verify(exe_id: str, typed: str, expected: str | None) -> None:
    """Field validation lives in the UI; the *step sequence* lives in the machine."""
    if typed and typed == expected:
        runner.process(exe_id, Event(kind="Verified"))
    else:
        ui.notify("That code does not match.", type="warning")
    wizard_ui.refresh()


def _reset() -> None:
    app.storage.user.pop("exe_id", None)
    wizard_ui.refresh()


@ui.refreshable
def wizard_ui() -> None:
    exe = _execution()
    ctx = exe.context
    step = exe.active_path

    with ui.card().classes("w-96"):
        ui.label(f"Step: {step}").classes("text-xs text-gray-500")

        if step == "Account":
            email = ui.input("Email", value=ctx.get("email", "")).classes("w-full")
            ui.button("Next", on_click=lambda: _advance(exe.id, "Next", email=email.value))

        elif step == "Profile":
            name = ui.input("Full name", value=ctx.get("full_name", "")).classes("w-full")
            with ui.row():
                ui.button("Back", on_click=lambda: _advance(exe.id, "Back")).props("flat")
                ui.button("Next", on_click=lambda: _advance(exe.id, "Next", full_name=name.value))

        elif step == "Verify":
            ui.label(f"We emailed a code to {ctx.get('email', '')}:")
            ui.label(ctx.get("code", "------")).classes("text-2xl font-mono")  # demo: shown inline
            code = ui.input("Verification code").classes("w-full")
            with ui.row():
                ui.button("Back", on_click=lambda: _advance(exe.id, "Back")).props("flat")
                ui.button("Verify", on_click=lambda: _verify(exe.id, code.value, ctx.get("code")))

        elif step == "Done":
            ui.label("🎉 All set — your account is ready.").classes("text-green-600 text-lg")
            ui.label(f"email = {ctx.get('email')}  ·  name = {ctx.get('full_name')}")
            ui.button("Start over", on_click=_reset).props("flat")


@ui.page("/")
def index() -> None:
    ui.label("Durable onboarding wizard").classes("text-xl font-bold")
    ui.label("Reload the page — or restart the server — and you resume right here.").classes(
        "text-sm text-gray-500"
    )
    with ui.row().classes("gap-8 items-start mt-4"):
        wizard_ui()
        with ui.card():
            ui.label("The statechart").classes("text-xs text-gray-500")
            ui.mermaid(DIAGRAM)


def main() -> None:
    ui.run(storage_secret="harel-wizard-demo", title="harel · durable wizard", reload=False)


# `python -m examples.nicegui_wizard.app` runs under __mp_main__/__main__ in NiceGUI.
if __name__ in {"__main__", "__mp_main__"}:
    main()
