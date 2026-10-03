"""The stable API for writing a `Transport` backend outside harel.

- `Transport` / `AsyncTransport` — the protocols (sync / async): a queue per group, with at
  most one message of a group in flight (single active consumer).
- `Lease` — a claimed message, as `claim` returns it and `ack`/`nack` receive it.
- `PARKED` — the `locked_by` a backend may store for a message parked by `nack(delay > 0)`:
  not claimable, its group blocked, until the delay passes.

Check a backend against the protocol with `harel.testing`.
"""

from harel.engine.aio_transport._base import AsyncTransport
from harel.engine.transport._base import _PARKED as PARKED
from harel.engine.transport._base import Lease, Transport

__all__ = ["AsyncTransport", "Lease", "PARKED", "Transport"]
