# Upgrading to 0.7

0.7 names everything a backend creates with a `prefix`, default `"harel"` (see [naming and
schema ownership](stores.md#naming-and-schema-ownership)). Tables, keys and collections created
by an earlier version have other names, so a 0.7 backend pointed at an existing database starts
**empty** next to the old data. Move the data once, with every worker and runner stopped, by
renaming the old names to the new ones — no data is copied.

## SQLite, libSQL, rqlite

Rename the tables; drop the transport's old indexes (the backend creates the new ones when it
starts). Run only the transport part where the transport lives in that database. On rqlite, send
the same statements to `/db/execute` in one request.

```python
import json
import sqlite3
import tempfile
from pathlib import Path

from harel import Event, Execution
from harel.engine.store import SqliteStore
from harel.engine.transport import SqliteTransport

UPGRADE_TO_0_7 = [
    "ALTER TABLE executions RENAME TO harel_executions",
    "ALTER TABLE outbox RENAME TO harel_outbox",
    "ALTER TABLE processed_events RENAME TO harel_processed_events",
    "ALTER TABLE timers RENAME TO harel_timers",
    "ALTER TABLE spawns RENAME TO harel_spawns",
    "ALTER TABLE trace RENAME TO harel_trace",
    # the transport, where it lives in this database
    "DROP INDEX IF EXISTS groups_by_last_claimed",
    "DROP INDEX IF EXISTS messages_by_group",
    "ALTER TABLE messages RENAME TO harel_transport_messages",
    "ALTER TABLE groups RENAME TO harel_transport_groups",
]

# a database as 0.6 left it: an execution, and a message waiting in the queue
db = Path(tempfile.mkdtemp()) / "state.db"
old = sqlite3.connect(db)
old.executescript("""
CREATE TABLE executions (id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INTEGER NOT NULL);
CREATE TABLE outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL);
CREATE TABLE processed_events (execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id));
CREATE TABLE timers (execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at REAL NOT NULL, PRIMARY KEY (execution_id, path));
CREATE TABLE spawns (seq INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, root_path TEXT NOT NULL, context TEXT NOT NULL);
CREATE TABLE trace (execution_id TEXT NOT NULL, idx INTEGER NOT NULL, entry TEXT NOT NULL, PRIMARY KEY (execution_id, idx));
CREATE TABLE messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, event TEXT NOT NULL, locked_by TEXT, lock_expiry REAL);
CREATE TABLE groups (group_id TEXT PRIMARY KEY, last_claimed_at REAL NOT NULL DEFAULT 0.0, priority INT NOT NULL DEFAULT 0);
CREATE INDEX groups_by_last_claimed ON groups (last_claimed_at);
CREATE INDEX messages_by_group ON messages (group_id, seq);
""")
exe = Execution(id="order-1", definition_id="order", version=3)
old.execute("INSERT INTO executions VALUES (?, ?, ?, ?)", (exe.id, exe.definition_id, exe.model_dump_json(), 3))
old.execute("INSERT INTO messages (group_id, event) VALUES (?, ?)", ("order-1", Event(kind="Paid").model_dump_json()))
old.execute("INSERT INTO groups (group_id) VALUES ('order-1')")
old.commit()

for statement in UPGRADE_TO_0_7:  # the upgrade
    old.execute(statement)
old.commit()
old.close()

assert SqliteStore(db).load("order-1").version == 3  # the 0.7 store finds it
lease = SqliteTransport(db).claim("worker", 30)
assert lease is not None and lease.event.kind == "Paid"  # and the 0.7 transport its message
```

## Postgres

Rename the tables and the transport's indexes in one transaction. The functions keep their
names under the default prefix (`harel_commit_cas`, `harel_claim`, `harel_ack`); the backend
replaces their bodies when it starts — or, with `create_schema=False`, apply `sql_schema` after
the renames.

```sql
BEGIN;
ALTER TABLE executions RENAME TO harel_executions;
ALTER TABLE outbox RENAME TO harel_outbox;
ALTER TABLE processed_events RENAME TO harel_processed_events;
ALTER TABLE timers RENAME TO harel_timers;
ALTER TABLE spawns RENAME TO harel_spawns;
ALTER TABLE trace RENAME TO harel_trace;
-- the transport, where it lives in this database
ALTER TABLE transport_messages RENAME TO harel_transport_messages;
ALTER TABLE transport_groups RENAME TO harel_transport_groups;
ALTER INDEX transport_messages_group RENAME TO harel_transport_messages_by_group;
DROP INDEX IF EXISTS transport_groups_claimable;  -- replaced: the backend creates the index claim uses
DROP FUNCTION IF EXISTS harel_ack(text, bigint, text);  -- an old signature, if it is still there
COMMIT;
```

## Redis

The keys move from the `stm:` prefix to `harel:`. Either keep the old one — `prefix="stm"`
(`HAREL_PREFIX=stm` for the worker, which names the other backends it builds too) — or rename them:

```python
import fakeredis  # any redis-py client; fakeredis here, so the example runs

r = fakeredis.FakeStrictRedis()
r.set("stm:exe:order-1", "{}")  # a key as 0.6 left it

for key in r.scan_iter(match="stm:*"):  # the upgrade
    r.rename(key, b"harel:" + key[len(b"stm:"):])

assert r.exists("harel:exe:order-1")
```

## Mongo

Rename the collections (in the `db_name` database, default `harel`):

```text
db.executions.renameCollection("harel_executions")
db.counters.renameCollection("harel_counters")
db.stm_messages.renameCollection("harel_transport_messages")
db.stm_locks.renameCollection("harel_transport_locks")
db.stm_counters.renameCollection("harel_transport_counters")
```

## DynamoDB, SQS

DynamoDB's tables already carried the `harel` prefix: nothing to do. The SQS transport's default
queue is now `harel.fifo`: keep the old one with `queue_name="stm.fifo"` (`HAREL_SQS_QUEUE`), or let
it drain before switching.

## Environment variables

Every variable the worker, the CLI and the monitor read moved from `STM_` to `HAREL_`, with the
rest of the name unchanged: `STM_STORE_BACKEND` is `HAREL_STORE_BACKEND`, `STM_POSTGRES_DSN` is
`HAREL_POSTGRES_DSN`, and so on for every one (the [CLI](cli.md) and [distribution](distribution.md)
pages list them). An old name still set is not read; at startup each is logged with the name to use.
`HAREL_PREFIX` and `HAREL_CREATE_SCHEMA` are new.

## Constructors

The backends that took a positional `prefix` take it by keyword now, next to `create_schema`:
`RedisStore(client, prefix=...)`, `RedisTransport(client, clock, prefix=...)`,
`PostgresTransport(conn, clock, prefix=...)`, `MongoTransport(client, db_name, clock,
prefix=...)`, `DynamoDBStore(client, prefix=...)` and their async and `from_url` / `from_dsn` /
`create` counterparts.
