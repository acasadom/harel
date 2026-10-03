"""Runnable place-order example.

    uv run python -m examples.place_order.run

Loads the declarative order machine (DSL), prints its PlantUML, and drives several
event sequences through the headless `DurableRunner`, printing the active state after
each event plus the final status and the recorded history.

The last two scenarios use `simulate_*_error` context flags to make actions raise
deliberately, showing how `on error` routes unexpected exceptions to a failed terminal
instead of dead-lettering the execution.
"""

from pathlib import Path

from harel import DictStore, DurableRunner, Event, definition_from_dsl_file, render

ORDER_STM = Path(__file__).parent / "order.stm"

# what every order starts with: no declines yet, one retry after a decline
NEW_ORDER = {"declines": 0, "max_retries": 1}
DECLINED = ("PaymentDeclined", {"reason": "insufficient funds"})
TO_DELIVERY = ["Picked", "Packed", "Dispatched", "Delivered"]

SCENARIOS = [
    ("happy path (express)", ["PlaceOrder", "PaymentAuthorized", *TO_DELIVERY], {}),
    (
        "international parcel (standard)",
        ["PlaceOrder", "PaymentAuthorized", *TO_DELIVERY],
        {"destination": "FR", "weight_kg": 2.5},
    ),
    ("payment retried, then paid", ["PlaceOrder", DECLINED, "PaymentAuthorized", *TO_DELIVERY], {}),
    ("payment keeps failing -> cancelled", ["PlaceOrder", DECLINED, DECLINED], {}),
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


def run_scenario(defn, name: str, events: list, context: dict):
    """Drive one scenario on a fresh in-memory store; print each step; return the
    final Execution. An event is a kind, or a (kind, data) pair."""
    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id, context={**NEW_ORDER, **context})
    print(f"\n=== {name} ===")
    print(f"  (start)              -> {exe.active_path}")
    for item in events:
        kind, data = item if isinstance(item, tuple) else (item, {})
        exe = runner.process(exe.id, Event(kind=kind, data=data))
        print(f"  {kind:<20} -> {exe.active_path}")
    print(f"  status={exe.status.name}  outcome={exe.outcome or '—'}")
    print("  history: " + " | ".join(exe.context.get("history", [])))
    return exe


def main() -> None:
    defn = definition_from_dsl_file(ORDER_STM, "order", validate=True)
    print("PlantUML\n--------")
    print(render(defn))
    for name, events, context in SCENARIOS:
        run_scenario(defn, name, events, context)


if __name__ == "__main__":
    main()
