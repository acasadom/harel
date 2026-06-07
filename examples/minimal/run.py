"""The smallest runnable harel example — drive the approval machine by hand.

    uv run python -m examples.minimal.run

Loads the DSL machine, starts it on an in-memory store, and feeds it a couple of
events, printing the active state after each one plus the final verdict. No actions,
no backend setup — just the machine and the headless runner.
"""

from pathlib import Path

from harel import DictStore, DurableRunner, Event, definition_from_dsl_file

APPROVAL_STM = Path(__file__).parent / "approval.stm"


def main() -> None:
    defn = definition_from_dsl_file(APPROVAL_STM, "approval", validate=True)
    runner = DurableRunner(DictStore(), {defn.id: defn})

    exe = runner.create(defn.id)
    print(f"(start)  -> {exe.active_path}")
    for kind in ("Submit", "Approve"):
        exe = runner.process(exe.id, Event(kind=kind))
        print(f"{kind:<8} -> {exe.active_path}")
    print(f"status: {exe.status.name}  outcome: {exe.outcome}")


if __name__ == "__main__":
    main()
