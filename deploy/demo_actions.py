"""Actions for the demo machines run by the docker-compose worker stack.

Referenced by dotted name from the `.stm` files in ``definitions/`` (``demo_actions.rec``)
and resolved at run time by the engine. Kept out of ``test/`` so the worker image
doesn't need test code on its path; both the workers (in containers) and the
integration test (on the host) put ``deploy/`` on PYTHONPATH so this resolves on
either side.
"""


def rec(stm, event, at=None, **kw):
    """Append `at` to ``execution_ctx["trace"]`` (the observable the test asserts)."""
    stm.execution_ctx.setdefault("trace", []).append(at)


def always_retry(stm, event, **kw):
    """Selector for the retry-budget demo: the attempt never succeeds, so the
    composite's overall timeout (budget) is what eventually ends it."""
    stm.execution_ctx.setdefault("trace", []).append("attempt")
    return "retry"


def boom(stm, event, **kw):
    """An action with an unhandled bug — to prove the worker fails the execution
    (status FAILED) instead of crashing."""
    raise RuntimeError("kaboom")
