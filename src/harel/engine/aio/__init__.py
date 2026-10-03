"""Async engine: the primary implementation.

The sync public API (`harel.Driver` / `harel.DurableRunner` / `DistributedRunner`) runs on
the runners here by default (`execution="background"`), bridged by an `anyio` BlockingPortal
(one background event loop). The pure engine (`harel.engine.core`) is unchanged — these
modules run the driver's flows (`harel.engine.driving`, `harel.engine.hosting`) with
coroutines, awaiting the action and the store/transport.
"""
