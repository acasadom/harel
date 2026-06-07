"""Async engine: the primary implementation.

The sync public API (`harel.Driver` / `harel.DurableRunner` / `DistributedRunner`) is a
thin facade over the runners here, bridged by an `anyio` BlockingPortal (one background
event loop). The pure engine (`harel.engine.core`) is unchanged — these modules are the
async *shell* that interprets its effect stream, awaiting the action and the store/transport.

Populated incrementally (see the migration plan). Nothing outside re-exports this yet.
"""
