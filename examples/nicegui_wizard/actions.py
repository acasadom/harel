"""Actions for the durable wizard example.

Every action has the engine's contract ``(stm, event, **inputs)`` and may read or
mutate the wizard's context (``stm.execution_ctx``). It is referenced from
``wizard.stm`` as a literal dotted path, so the runtime imports it lazily. (Keeping
what the user typed needs no action: the transitions `set` it into the context.)
"""

import random


def send_code(stm, event, **kw) -> None:
    """Simulate emailing a verification code; stash it in the context.

    A real app would email it; the demo UI shows it inline so you can complete the
    flow. Re-entering ``Verify`` (Back then Next) issues a fresh code.
    """
    stm.execution_ctx["code"] = f"{random.randint(0, 999999):06d}"
