"""A small library of reusable, parameterized actions — the "composables" a
statechart can reference by dotted name without rewriting common logic.

These are ordinary actions (signature ``(stm, event, **inputs)``); the engine has
no special knowledge of them. They are policy (program), kept out of the engine.

`exponential_backoff` / `linear_backoff` are the canonical pair for a retry
composite: an `on_enter` computes the next wait into the context, and the state's
``timeout: {context: <into>}`` arms a durable timer with it — so backoff is
declarative-flavoured (configured via ``inputs``) and visible, while the engine
stays a pure timer (the delay is just a number it reads from the context).

Example (in a Waiting state of a retry loop)::

    Waiting:
      on_enter: {function: harel.lib.exponential_backoff,
                 inputs: {base: 5, factor: 2, cap: 600, into: backoff}}
      timeout: {context: backoff}
      transitions:
        - {to: Send, on_event: {type: Timeout}}   # the retry edge (visible)

The attempt counter lives in the context (it must survive the re-entry loop), so
`reset_backoff` is provided to clear it on the success path if desired.
"""

from typing import Any, Optional


def _next_backoff(stm: Any, base: float, step_fn, cap: Optional[float], into: str, counter: str) -> float:
    ctx = stm.execution_ctx
    n = int(ctx.get(counter, 0))
    delay = step_fn(base, n)
    if cap is not None:
        delay = min(delay, float(cap))
    ctx[counter] = n + 1
    ctx[into] = delay
    return delay


def exponential_backoff(
    stm: Any,
    event: Any,
    base: float = 1.0,
    factor: float = 2.0,
    cap: Optional[float] = None,
    into: str = "backoff",
    counter: str = "__attempt",
    **kw: Any,
) -> float:
    """Compute the next exponential wait (`base * factor**attempt`, capped) into
    ``context[into]`` and bump the attempt counter. Returns the delay."""
    return _next_backoff(stm, base, lambda b, n: b * (factor**n), cap, into, counter)


def linear_backoff(
    stm: Any,
    event: Any,
    base: float = 1.0,
    step: float = 1.0,
    cap: Optional[float] = None,
    into: str = "backoff",
    counter: str = "__attempt",
    **kw: Any,
) -> float:
    """Compute the next linear wait (`base + step*attempt`, capped) into
    ``context[into]`` and bump the attempt counter. Returns the delay."""
    return _next_backoff(stm, base, lambda b, n: b + step * n, cap, into, counter)


def reset_backoff(stm: Any, event: Any, counter: str = "__attempt", **kw: Any) -> None:
    """Clear the attempt counter (e.g. on the success path, so a later retry of the
    same execution starts fresh)."""
    stm.execution_ctx.pop(counter, None)


def join_success(stm: Any, event: Any, mode: str = "all", **kw: Any) -> str:
    """Selector over an orthogonal join's ``region_results``: returns ``"pass"`` if
    the regions' verdicts satisfy ``mode`` (``"all"`` => every region's outcome is
    ``"success"``, ``"any"`` => at least one), else ``"fail"``. The DSL sugar
    ``from Fork join all|any to X else to Y`` desugars to a selector calling this
    (``{"pass": X, else: Y}``); the model still writes its own selector for anything
    richer. Empty region results => ``"fail"`` (nothing succeeded)."""
    results = stm.execution_ctx.get("region_results", {})
    oks = [r.get("outcome") == "success" for r in results.values()]
    passed = bool(oks) and (all(oks) if mode == "all" else any(oks))
    return "pass" if passed else "fail"
