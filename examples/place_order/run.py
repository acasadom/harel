"""Runnable place-order example.

    uv run python -m examples.place_order.run

Loads the declarative order machine (DSL), prints its PlantUML, and drives a few
event sequences through the headless `DurableRunner` over an in-memory store,
printing the active state after each event plus the final status and the recorded
history.
"""

from pathlib import Path

from harel import DictStore, DurableRunner, Event, definition_from_dsl_file, render

ORDER_STM = Path(__file__).parent / "order.stm"

SCENARIOS = [
    ("happy path", ["PlaceOrder", "PaymentAuthorized", "Picked", "Packed", "Dispatched", "Delivered"]),
    (
        "payment retried, then paid",
        ["PlaceOrder", "PaymentDeclined", "PaymentAuthorized", "Picked", "Packed", "Dispatched", "Delivered"],
    ),
    ("payment keeps failing -> cancelled", ["PlaceOrder", "PaymentDeclined", "PaymentDeclined"]),
    ("cancelled while awaiting payment", ["PlaceOrder", "CancelOrder"]),
]


def run_scenario(name: str, events: list[str]) -> None:
    defn = definition_from_dsl_file(ORDER_STM, "order")
    runner = DurableRunner(DictStore(), {defn.id: defn})

    exe = runner.create(defn.id)
    print(f"\n=== {name} ===")
    print(f"  (start)              -> {exe.active_path}")
    for kind in events:
        exe = runner.process(exe.id, Event(kind=kind))
        print(f"  {kind:<20} -> {exe.active_path}")

    print(f"  final status: {exe.status.name}")
    print("  history: " + " | ".join(exe.context.get("history", [])))


def main() -> None:
    defn = definition_from_dsl_file(ORDER_STM, "order")
    print("PlantUML\n--------")
    print(render(defn))
    for name, events in SCENARIOS:
        run_scenario(name, events)


if __name__ == "__main__":
    main()
