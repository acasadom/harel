"""Contract checks for a store or transport backend written outside harel.

harel runs these same checks on every backend it ships; a third-party backend runs them on
itself — one call per contract — and learns that it meets the protocol, or what breaks it:

```text
from harel.testing import assert_listing_contract, assert_outbox_contract, assert_purge_contract
from harel.testing import assert_definitions_contract, assert_transport_contract

def test_my_store_meets_the_protocol():
    assert_listing_contract(MyStore(), ordered=True)
    assert_purge_contract(MyStore())
    assert_outbox_contract(MyStore())
    assert_definitions_contract(MyStore())
    assert_pending_work_contract(MyStore())

def test_my_transport_meets_the_protocol():
    assert_transport_contract(lambda clock: MyTransport(clock=clock))
    assert_machine_routing_contract(lambda clock: MyTransport(clock=clock))  # or ..._refuses_...
```

Each has an `assert_async_*` twin for an async backend. The store contracts take a `ns`
prefix for the ids they seed, so they run on a backend shared with other data; the transport
contract builds a fresh transport for each check. They use plain `assert`s, so any test
runner works.
"""

from harel.testing.definitions import assert_async_definitions_contract, assert_definitions_contract
from harel.testing.listing import assert_async_listing_contract, assert_listing_contract
from harel.testing.outbox import assert_async_outbox_contract, assert_outbox_contract
from harel.testing.pending import assert_async_pending_work_contract, assert_pending_work_contract
from harel.testing.purge import assert_async_purge_contract, assert_purge_contract
from harel.testing.routing import (
    assert_async_machine_routing_contract,
    assert_async_refuses_machine_routing,
    assert_machine_routing_contract,
    assert_refuses_machine_routing,
)
from harel.testing.transport import assert_async_transport_contract, assert_transport_contract

__all__ = [
    "assert_async_pending_work_contract",
    "assert_pending_work_contract",
    "assert_async_machine_routing_contract",
    "assert_async_refuses_machine_routing",
    "assert_machine_routing_contract",
    "assert_refuses_machine_routing",
    "assert_async_definitions_contract",
    "assert_definitions_contract",
    "assert_async_listing_contract",
    "assert_async_outbox_contract",
    "assert_async_purge_contract",
    "assert_async_transport_contract",
    "assert_listing_contract",
    "assert_outbox_contract",
    "assert_purge_contract",
    "assert_transport_contract",
]
