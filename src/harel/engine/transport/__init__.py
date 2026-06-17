"""The event transport seam (a single-active-consumer-per-group queue).

Split into the shared contract (`_base`) and one module per backend; everything is
re-exported here, so `from harel.engine.transport import RedisTransport` (etc.) and
`from harel.engine.transport import _PARKED, Lease` keep working unchanged.
"""

from harel.engine.transport._base import _CLAIM_LUA, _PARKED, Lease, Transport
from harel.engine.transport.inmemory import InMemoryTransport
from harel.engine.transport.libsql import LibsqlTransport
from harel.engine.transport.mongo import MongoTransport
from harel.engine.transport.postgres import PostgresTransport
from harel.engine.transport.redis import RedisTransport
from harel.engine.transport.rqlite import RqliteTransport
from harel.engine.transport.sqlite import SqliteTransport
from harel.engine.transport.sqs import SqsTransport

__all__ = [
    "Transport",
    "Lease",
    "_PARKED",
    "_CLAIM_LUA",
    "InMemoryTransport",
    "SqliteTransport",
    "LibsqlTransport",
    "RedisTransport",
    "PostgresTransport",
    "RqliteTransport",
    "SqsTransport",
    "MongoTransport",
]
