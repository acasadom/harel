"""Runnable place-order example.

    uv run python -m examples.place_order.run

Loads the declarative order machine (DSL), prints its PlantUML, and drives several
event sequences through the headless `DurableRunner`, printing the active state after
each event plus the final status and the recorded history.

The last two scenarios use `simulate_*_error` context flags to make actions raise
deliberately, showing how `on error` routes unexpected exceptions to a failed terminal
instead of crashing the execution.
"""

from pathlib import Path

from harel import DictStore, DurableRunner, Event, definition_from_dsl_file, render

ORDER_STM = Path(__file__).parent / "order.stm"

SCENARIOS = [
    ("happy path", ["PlaceOrder", "PaymentAuthorized", "Picked", "Packed", "Dispatched", "Delivered"], {}),
    (
        "payment retried, then paid",
        ["PlaceOrder", "PaymentDeclined", "PaymentAuthorized", "Picked", "Packed", "Dispatched", "Delivered"],
        {},
    ),
    ("payment keeps failing -> cancelled", ["PlaceOrder", "PaymentDeclined", "PaymentDeclined"], {}),
    ("cancelled while awaiting payment", ["PlaceOrder", "CancelOrder"], {}),
    # on error: capture_payment raises (gateway timeout) -> PaymentError terminal
    (
        "capture_payment raises -> PaymentError",
        ["PlaceOrder", "PaymentAuthorized"],
        {"simulate_payment_error": True},
    ),
    # on error: pick raises (warehouse API down) -> FulfilmentError terminal
    (
        "pick raises -> FulfilmentError",
        ["PlaceOrder", "PaymentAuthorized"],
        {"simulate_fulfilment_error": True},
    ),
]


def run_scenario(defn, name: str, events: list[str], context: dict) -> None:
    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id, context=context)
    print(f"\n=== {name} ===")
    print(f"  (start)              -> {exe.active_path}")
    for kind in events:
        exe = runner.process(exe.id, Event(kind=kind))
        print(f"  {kind:<20} -> {exe.active_path}")
    print(f"  status={exe.status.name}  outcome={exe.outcome or '—'}")
    print("  history: " + " | ".join(exe.context.get("history", [])))


def main() -> None:
    defn = definition_from_dsl_file(ORDER_STM, "order")
    print("PlantUML\n--------")
    print(render(defn))
    for name, events, context in SCENARIOS:
        run_scenario(defn, name, events, context)


if __name__ == "__main__":
    main()
