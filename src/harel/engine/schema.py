"""The names and the schema of what harel's backends create, for one `prefix`.

Every persistent backend takes a `prefix` (default `"harel"`) and names everything it creates
with it — SQL tables, indexes and functions, Redis keys, Mongo collections, DynamoDB tables —
so several harel deployments, or harel and other applications, share one database without
colliding. `Names(prefix)` gives those names.

Every backend that has a schema also takes `create_schema` (default `True`): it creates its
tables (indexes, functions) when it is built, idempotently. With `create_schema=False` it
creates nothing and expects the schema to be there — when something else owns it, e.g. a
migration tool. `sql_schema(dialect, prefix)` is that schema for the SQL backends, as the
statements the backends themselves run: hand them to the migration tool.

`SCHEMA_VERSION` changes whenever the schema does (a table, a column, an index, a function),
and the CHANGELOG says how to move from one version to the next.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cached_property

SCHEMA_VERSION = 1
DEFAULT_PREFIX = "harel"

# an SQL identifier; at most 30 characters, so the longest name built from it (an index) stays
# within Postgres' 63 (longer ones are silently truncated there, and could then collide)
_PREFIX = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,29}")

DIALECTS = ("sqlite", "postgres")


@dataclass(frozen=True)
class Names:
    """The name of everything a backend creates under `prefix`. `sql(template)` fills a
    statement written with `{executions}`, `{outbox}`, ... placeholders."""

    prefix: str = DEFAULT_PREFIX

    def __post_init__(self) -> None:
        if not isinstance(self.prefix, str) or not _PREFIX.fullmatch(self.prefix):
            raise ValueError(
                f"prefix must be an identifier (a letter or _, then letters, digits or _; "
                f"at most 30 characters), got {self.prefix!r}"
            )

    # --- the store ---------------------------------------------------------------------------
    @property
    def executions(self) -> str:
        return f"{self.prefix}_executions"

    @property
    def outbox(self) -> str:
        return f"{self.prefix}_outbox"

    @property
    def processed_events(self) -> str:
        return f"{self.prefix}_processed_events"

    @property
    def spawns(self) -> str:
        return f"{self.prefix}_spawns"

    @property
    def timers(self) -> str:
        return f"{self.prefix}_timers"

    @property
    def trace(self) -> str:
        return f"{self.prefix}_trace"

    @property
    def counters(self) -> str:
        """The outbox/spawn sequence allocator (the document stores)."""
        return f"{self.prefix}_counters"

    # --- the transport -----------------------------------------------------------------------
    @property
    def groups(self) -> str:
        return f"{self.prefix}_transport_groups"

    @property
    def messages(self) -> str:
        return f"{self.prefix}_transport_messages"

    @property
    def transport_locks(self) -> str:
        """The per-group locks (the Mongo transport)."""
        return f"{self.prefix}_transport_locks"

    @property
    def transport_counters(self) -> str:
        """The message sequence allocator (the Mongo transport)."""
        return f"{self.prefix}_transport_counters"

    # --- Postgres functions ------------------------------------------------------------------
    @property
    def commit_cas(self) -> str:
        return f"{self.prefix}_commit_cas"

    @property
    def claim(self) -> str:
        return f"{self.prefix}_claim"

    @property
    def ack(self) -> str:
        return f"{self.prefix}_ack"

    @cached_property
    def _placeholders(self) -> dict[str, str]:
        keys = (
            "executions outbox processed_events spawns timers trace groups messages "
            "commit_cas claim ack prefix"
        )
        return {k: getattr(self, k) for k in keys.split()}

    def sql(self, template: str) -> str:
        """`template` with its `{executions}`, `{outbox}`, ..., `{prefix}` placeholders filled."""
        return template.format_map(self._placeholders)


def _check_dialect(dialect: str) -> None:
    if dialect not in DIALECTS:
        raise ValueError(f"dialect must be one of {DIALECTS}, got {dialect!r}")


def store_schema(dialect: str, prefix: str = DEFAULT_PREFIX) -> list[str]:
    """The statements that create an SQL store's schema under `prefix`, idempotently.
    `dialect` is `"sqlite"` (SQLite, libSQL, rqlite) or `"postgres"`."""
    _check_dialect(dialect)
    names = Names(prefix)
    return [names.sql(t) for t in (_PG_STORE if dialect == "postgres" else _SQLITE_STORE)]


def transport_schema(dialect: str, prefix: str = DEFAULT_PREFIX) -> list[str]:
    """The statements that create an SQL transport's schema under `prefix`, idempotently."""
    _check_dialect(dialect)
    names = Names(prefix)
    return [names.sql(t) for t in (_PG_TRANSPORT if dialect == "postgres" else _SQLITE_TRANSPORT)]


def sql_schema(dialect: str, prefix: str = DEFAULT_PREFIX) -> list[str]:
    """The whole SQL schema under `prefix` — the store's and the transport's — as the
    statements the backends run: what a migration tool applies when it owns the schema
    (`create_schema=False`). Each is idempotent (`IF NOT EXISTS`, `CREATE OR REPLACE`)."""
    return store_schema(dialect, prefix) + transport_schema(dialect, prefix)


# --- SQLite dialect (SQLite, libSQL, rqlite) --------------------------------------------------
_SQLITE_STORE = (
    "CREATE TABLE IF NOT EXISTS {executions} "
    "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS {outbox} "
    "(seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS {processed_events} "
    "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))",
    "CREATE TABLE IF NOT EXISTS {timers} "
    "(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at REAL NOT NULL, "
    "PRIMARY KEY (execution_id, path))",
    "CREATE TABLE IF NOT EXISTS {spawns} "
    "(seq INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
    "root_path TEXT NOT NULL, context TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS {trace} "
    "(execution_id TEXT NOT NULL, idx INTEGER NOT NULL, entry TEXT NOT NULL, "
    "PRIMARY KEY (execution_id, idx))",
)

_SQLITE_TRANSPORT = (
    "CREATE TABLE IF NOT EXISTS {messages} "
    "(seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, event TEXT NOT NULL, "
    "locked_by TEXT, lock_expiry REAL)",
    "CREATE TABLE IF NOT EXISTS {groups} "
    "(group_id TEXT PRIMARY KEY, last_claimed_at REAL NOT NULL DEFAULT 0.0, "
    "priority INT NOT NULL DEFAULT 0)",
    "CREATE INDEX IF NOT EXISTS {prefix}_transport_groups_by_last_claimed ON {groups} (last_claimed_at)",
    "CREATE INDEX IF NOT EXISTS {prefix}_transport_messages_by_group ON {messages} (group_id, seq)",
)

# --- Postgres -----------------------------------------------------------------------------------
_PG_STORE = (
    "CREATE TABLE IF NOT EXISTS {executions} "
    "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS {outbox} (seq BIGSERIAL PRIMARY KEY, target_id TEXT, event TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS {processed_events} "
    "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))",
    "CREATE TABLE IF NOT EXISTS {timers} "
    "(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at DOUBLE PRECISION NOT NULL, "
    "PRIMARY KEY (execution_id, path))",
    "CREATE TABLE IF NOT EXISTS {spawns} "
    "(seq BIGSERIAL PRIMARY KEY, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
    "root_path TEXT NOT NULL, context TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS {trace} "
    "(execution_id TEXT NOT NULL, idx INT NOT NULL, entry TEXT NOT NULL, PRIMARY KEY (execution_id, idx))",
    # The state-only commit fast path, the analog of Redis' CAS script: for an event that only
    # advances state (no emits/spawns/timers/trace) it does the version-CAS + write (+ dedupe) in
    # ONE server-side round-trip. Returns true on commit, false on a version conflict (no RAISE,
    # so the transaction isn't aborted — the caller rolls back and raises StoreConflict).
    """CREATE OR REPLACE FUNCTION {commit_cas}(p_id text, p_defn text, p_data text, p_old bigint, p_event text)
RETURNS boolean AS $$
DECLARE n int; row_exists boolean;
BEGIN
  UPDATE {executions} SET data = p_data, version = p_old + 1 WHERE id = p_id AND version = p_old;
  GET DIAGNOSTICS n = ROW_COUNT;
  IF n = 0 THEN
    SELECT EXISTS (SELECT 1 FROM {executions} WHERE id = p_id) INTO row_exists;
    IF p_old = 0 AND NOT row_exists THEN
      INSERT INTO {executions} (id, definition_id, data, version) VALUES (p_id, p_defn, p_data, 1);
    ELSE
      RETURN false;
    END IF;
  END IF;
  IF p_event <> '' THEN
    INSERT INTO {processed_events} (execution_id, event_id) VALUES (p_id, p_event) ON CONFLICT DO NOTHING;
  END IF;
  RETURN true;
END; $$ LANGUAGE plpgsql""",
)

_PG_TRANSPORT = (
    "CREATE TABLE IF NOT EXISTS {messages} "
    "(seq BIGSERIAL PRIMARY KEY, group_id TEXT NOT NULL, event TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS {prefix}_transport_messages_by_group ON {messages} (group_id, seq)",
    "CREATE TABLE IF NOT EXISTS {groups} "
    "(group_id TEXT PRIMARY KEY, locked_by TEXT, lock_expiry DOUBLE PRECISION, "
    "priority INT NOT NULL DEFAULT 0)",
    "CREATE INDEX IF NOT EXISTS {prefix}_transport_groups_claimable ON {groups} (lock_expiry)",
    # `claim` and `ack` are PL/pgSQL functions, the Postgres analog of the Redis Lua scripts: the
    # transport is round-trip-bound, and each folds an operation's statements into one
    # server-side call. The lease itself is atomic either way (FOR UPDATE SKIP LOCKED).
    #
    # claim: lock the first claimable group (unlocked or lease expired, at least `p_min_priority`,
    # least recently served first) with SKIP LOCKED, and return its head message; a group left
    # with no messages is dropped and the next one tried
    """CREATE OR REPLACE FUNCTION {claim}(p_now double precision, p_lease double precision, p_token text, p_min_priority int DEFAULT 0)
RETURNS TABLE(group_id text, seq bigint, event text) AS $$
DECLARE g text;
BEGIN
  LOOP
    UPDATE {groups} tg SET locked_by = p_token, lock_expiry = p_lease
    WHERE tg.group_id = (
      SELECT s.group_id FROM {groups} s
      WHERE (s.locked_by IS NULL OR s.lock_expiry < p_now)
        AND s.priority >= p_min_priority
      ORDER BY COALESCE(s.lock_expiry, 0) ASC, s.group_id FOR UPDATE SKIP LOCKED LIMIT 1
    ) RETURNING tg.group_id INTO g;
    IF g IS NULL THEN RETURN; END IF;
    RETURN QUERY SELECT m.group_id, m.seq, m.event FROM {messages} m
                 WHERE m.group_id = g ORDER BY m.seq LIMIT 1;
    IF FOUND THEN RETURN; END IF;
    DELETE FROM {groups} WHERE {groups}.group_id = g AND locked_by = p_token;
  END LOOP;
END; $$ LANGUAGE plpgsql""",
    # ack: fenced by the lease token — remove the message, then free the group (it goes to the
    # back of the round-robin) or, drained, delete it so its priority resets on the next publish
    """CREATE OR REPLACE FUNCTION {ack}(p_group text, p_seq bigint, p_token text, p_now double precision)
RETURNS void AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM {groups} WHERE group_id = p_group AND locked_by = p_token) THEN
    DELETE FROM {messages} WHERE seq = p_seq;
    IF EXISTS (SELECT 1 FROM {messages} WHERE group_id = p_group) THEN
      UPDATE {groups} SET locked_by = NULL, lock_expiry = p_now
      WHERE group_id = p_group AND locked_by = p_token;
    ELSE
      DELETE FROM {groups} WHERE group_id = p_group AND locked_by = p_token;
    END IF;
  END IF;
END; $$ LANGUAGE plpgsql""",
)
