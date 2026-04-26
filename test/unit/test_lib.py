"""The shipped composable actions (harel.lib): backoff helpers."""

from harel import lib


class _Stm:
    def __init__(self, ctx=None):
        self.execution_ctx = ctx if ctx is not None else {}


def test_exponential_backoff_grows_and_caps():
    stm = _Stm()
    assert lib.exponential_backoff(stm, None, base=5, factor=2, cap=30) == 5  # 5*2**0
    assert lib.exponential_backoff(stm, None, base=5, factor=2, cap=30) == 10  # 5*2**1
    assert lib.exponential_backoff(stm, None, base=5, factor=2, cap=30) == 20  # 5*2**2
    assert lib.exponential_backoff(stm, None, base=5, factor=2, cap=30) == 30  # 5*2**3=40 -> capped
    assert stm.execution_ctx["backoff"] == 30
    assert stm.execution_ctx["__attempt"] == 4


def test_linear_backoff_grows_and_caps():
    stm = _Stm()
    assert lib.linear_backoff(stm, None, base=1, step=3, cap=7) == 1  # 1+3*0
    assert lib.linear_backoff(stm, None, base=1, step=3, cap=7) == 4  # 1+3*1
    assert lib.linear_backoff(stm, None, base=1, step=3, cap=7) == 7  # 1+3*2=7
    assert lib.linear_backoff(stm, None, base=1, step=3, cap=7) == 7  # 1+3*3=10 -> capped


def test_reset_backoff_clears_the_counter():
    stm = _Stm()
    lib.exponential_backoff(stm, None, base=2)
    lib.exponential_backoff(stm, None, base=2)
    assert stm.execution_ctx["__attempt"] == 2
    lib.reset_backoff(stm, None)
    assert "__attempt" not in stm.execution_ctx
    assert lib.exponential_backoff(stm, None, base=2) == 2  # starts fresh (2*2**0)


def test_writes_into_custom_key():
    stm = _Stm()
    lib.exponential_backoff(stm, None, base=3, into="wait", counter="n")
    assert stm.execution_ctx["wait"] == 3
    assert stm.execution_ctx["n"] == 1
