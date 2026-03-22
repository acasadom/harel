"""Shared test actions referenced by STM YAML configs (resolved by dotted name).

Lives at the test root so it stays importable as ``stm_actions`` from any test
subdirectory. Hooks record their label in ``execution_ctx["trace"]``; `sel`
consumes the next branch key from ``context["picks"]``; `pick` returns the
``context["pick"]`` flag; `boom` raises (to exercise exception handling)."""


def _rec(label):
    def fn(stm, event, **kw):
        stm.execution_ctx.setdefault("trace", []).append(label)

    fn.__name__ = label
    return fn


# labeled recorders used by the engine-semantics configs
oe, ox = _rec("oe"), _rec("ox")
in1e, in1x, in2e = _rec("in1e"), _rec("in1x"), _rec("in2e")
ae, ax = _rec("ae"), _rec("ax")
de, dx, other_e = _rec("de"), _rec("dx"), _rec("other_e")
we, wa, ne = _rec("we"), _rec("wa"), _rec("ne")
l1e, l2e, leafe = _rec("l1e"), _rec("l2e"), _rec("leafe")


def rec(stm, event, at=None, **kw):
    """Generic hook: records the label passed via ``inputs.at``."""
    stm.execution_ctx.setdefault("trace", []).append(at)


def sel(stm, event, at="pick", **kw):
    """Selector: records `at` and returns the next branch key from `picks`."""
    stm.execution_ctx.setdefault("trace", []).append(at)
    return stm.execution_ctx["picks"].pop(0)


def pick(stm, event, **kw):
    """Selector: records "pick" and returns the `pick` flag from the context."""
    stm.execution_ctx.setdefault("trace", []).append("pick")
    return stm.execution_ctx.get("pick", True)


def boom(stm, event, **kw):
    raise RuntimeError("boom")


def noop(stm, event, **kw):
    pass


def h(label: str) -> str:
    """YAML hook fragment referencing `rec` with its label (terse battery YAML)."""
    return f"{{function: stm_actions.rec, inputs: {{at: {label}}}}}"
