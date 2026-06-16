"""Actions for the durable wizard example.

Every action has the engine's contract ``(stm, event, **inputs)`` and may read or
mutate the wizard's context (``stm.execution_ctx``). They are referenced from
``wizard.stm`` as literal dotted paths, so the runtime imports them lazily.
"""

import random


def save(stm, event, **kw) -> None:
    """Merge the fields carried on the triggering event into the context.

    The UI sends the typed field on the ``Next`` event; persisting it here means it
    survives Back/Next and — because the context is checkpointed to a durable store —
    a server restart. Empty values are ignored so a ``Back`` (no data) never wipes it.
    """
    data = getattr(event, "data", None) or {}
    stm.execution_ctx.update({k: v for k, v in data.items() if v not in (None, "")})


def send_code(stm, event, **kw) -> None:
    """Simulate emailing a verification code; stash it in the context.

    A real app would email it; the demo UI shows it inline so you can complete the
    flow. Re-entering ``Verify`` (Back then Next) issues a fresh code.
    """
    stm.execution_ctx["code"] = f"{random.randint(0, 999999):06d}"
