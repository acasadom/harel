"""The persistence seam for Executions.

An `ExecutionStore` is how the `Driver` reads and checkpoints the running
instances, plus a **transactional outbox** for the deferred events an Execution
emits. `commit` writes the Execution and its emitted events in the *same*
transaction, so a crash can never leave the state advanced but the `Finished`
unsent. A separate relay reads the outbox and delivers it after the commit.

This package splits the one-file store into the shared contract (`_base`) and one
module per backend. Everything is re-exported here, so importers keep using
`from harel.engine.store import RedisStore` (etc.) unchanged.
"""

from harel.engine.store._base import (
    ExecutionAlreadyExists,
    ExecutionStore,
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
)
from harel.engine.store.dict import DictStore
from harel.engine.store.dynamodb import DynamoDBStore
from harel.engine.store.libsql import LibsqlStore
from harel.engine.store.mongo import MongoStore
from harel.engine.store.postgres import PostgresStore
from harel.engine.store.redis import RedisStore
from harel.engine.store.rqlite import RqliteStore
from harel.engine.store.sqlite import SqliteStore

__all__ = [
    "ExecutionAlreadyExists",
    "ExecutionStore",
    "OutboxEntry",
    "SpawnEntry",
    "StoreConflict",
    "TimerOp",
    "DictStore",
    "SqliteStore",
    "LibsqlStore",
    "RedisStore",
    "PostgresStore",
    "RqliteStore",
    "MongoStore",
    "DynamoDBStore",
]
