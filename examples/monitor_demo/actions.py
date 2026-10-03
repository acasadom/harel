"""Actions of the monitor demo's Order machine (`machines/order.stm`).

They only touch the context, so the monitor has something to show in each trace step;
`charge_card` raises when the order says `decline`, to leave a real dead letter.
"""


def reserve_inventory(stm, event, **kw):
    stm.execution_ctx["reserved"] = True


def charge_card(stm, event, **kw):
    if stm.execution_ctx.get("decline"):
        raise RuntimeError("card declined (code 51)")
    stm.execution_ctx["charged"] = stm.execution_ctx["total"]


def notify_customer(stm, event, **kw):
    stm.execution_ctx["notified"] = True
