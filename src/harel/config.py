"""Centralised configuration — every `HAREL_*` environment variable, read in one place.

`Config.from_env()` parses the environment (names, defaults, int/float coercion) into a single
frozen object; the worker and the TUI build one and read attributes instead of scattering
`os.environ[...]` calls. It reads **at call time** (not import) so that callers/tests which set
env vars before invoking `build_*` still take effect — `from_env()` re-reads each time.

Backend-specific variables (e.g. `HAREL_POSTGRES_DSN`, only needed when the postgres backend is
selected) are `Optional` here; the builder validates the one it needs with `require()`.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Mapping, Optional

logger = logging.getLogger(__name__)
_warned: set[str] = set()  # the pre-0.7 names already logged by this process


@dataclass(frozen=True)
class Config:
    """A snapshot of the `HAREL_*` environment. Build with `Config.from_env()`."""

    # --- backend selection ---
    store_backend: str = "sqlite"  # HAREL_STORE_BACKEND
    transport_backend: str = "redis"  # HAREL_TRANSPORT_BACKEND
    # --- redis ---
    redis_url: Optional[str] = None  # HAREL_REDIS_URL (transport, and store fallback)
    store_redis_url: Optional[str] = None  # HAREL_STORE_REDIS_URL (store; falls back to redis_url)
    # --- sqlite ---
    store_db: Optional[str] = None  # HAREL_STORE_DB
    transport_db: Optional[str] = None  # HAREL_TRANSPORT_DB
    # --- postgres ---
    postgres_dsn: Optional[str] = None  # HAREL_POSTGRES_DSN
    # --- rqlite ---
    rqlite_url: Optional[str] = None  # HAREL_RQLITE_URL
    # --- mongo ---
    mongo_url: Optional[str] = None  # HAREL_MONGO_URL
    mongo_db: str = "harel"  # HAREL_MONGO_DB
    # --- libsql (Turso) ---
    libsql_db: Optional[str] = None  # HAREL_LIBSQL_DB
    libsql_sync_url: Optional[str] = None  # HAREL_LIBSQL_SYNC_URL (embedded replica)
    libsql_auth_token: str = ""  # HAREL_LIBSQL_AUTH_TOKEN
    # --- aws (dynamodb store + sqs transport) ---
    sqs_endpoint: Optional[str] = None  # HAREL_SQS_ENDPOINT
    sqs_queue: str = "harel.fifo"  # HAREL_SQS_QUEUE
    dynamodb_endpoint: Optional[str] = None  # HAREL_DYNAMODB_ENDPOINT
    aws_region: str = "us-east-1"  # HAREL_AWS_REGION
    # --- worker loop ---
    definitions_dir: Optional[str] = None  # HAREL_DEFINITIONS_DIR
    worker_id: Optional[str] = None  # HAREL_WORKER_ID (None -> caller defaults to the hostname)
    visibility: float = 30.0  # HAREL_VISIBILITY (lease seconds)
    concurrency: int = 256  # HAREL_CONCURRENCY (events in flight on the async loop)
    # --- monitoring TUI ---
    tui_interval_ms: int = 1000  # HAREL_TUI_INTERVAL_MS (auto-refresh)
    tui_theme: str = "nord"  # HAREL_TUI_THEME
    # --- execution trace (opt-in timeline) ---
    trace: bool = False  # HAREL_TRACE (record a per-event timeline step in commit)
    trace_max: int = 200  # HAREL_TRACE_MAX (ring: keep only the last N steps per execution)

    prefix: str = "harel"  # HAREL_PREFIX (names every table, key, collection the backends create)
    create_schema: bool = True  # HAREL_CREATE_SCHEMA ("0"/"false" when a migration tool owns it)

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "Config":
        """Read the `HAREL_*` variables from `env` (default `os.environ`) into a `Config`. A
        variable still set under its name from before 0.7 (`STM_*`) is not read; each is logged
        once, with the name to use."""
        e = os.environ if env is None else env
        for old in sorted(k for k in e if k.startswith("STM_") and k not in _warned):
            _warned.add(old)
            logger.warning(
                "%s is not read since harel 0.7: set %s instead", old, "HAREL_" + old[len("STM_") :]
            )
        return cls(
            store_backend=e.get("HAREL_STORE_BACKEND", "sqlite"),
            transport_backend=e.get("HAREL_TRANSPORT_BACKEND", "redis"),
            redis_url=e.get("HAREL_REDIS_URL"),
            store_redis_url=e.get("HAREL_STORE_REDIS_URL"),
            store_db=e.get("HAREL_STORE_DB"),
            transport_db=e.get("HAREL_TRANSPORT_DB"),
            postgres_dsn=e.get("HAREL_POSTGRES_DSN"),
            rqlite_url=e.get("HAREL_RQLITE_URL"),
            mongo_url=e.get("HAREL_MONGO_URL"),
            mongo_db=e.get("HAREL_MONGO_DB", "harel"),
            libsql_db=e.get("HAREL_LIBSQL_DB"),
            libsql_sync_url=e.get("HAREL_LIBSQL_SYNC_URL"),
            libsql_auth_token=e.get("HAREL_LIBSQL_AUTH_TOKEN", ""),
            sqs_endpoint=e.get("HAREL_SQS_ENDPOINT"),
            sqs_queue=e.get("HAREL_SQS_QUEUE", "harel.fifo"),
            dynamodb_endpoint=e.get("HAREL_DYNAMODB_ENDPOINT"),
            aws_region=e.get("HAREL_AWS_REGION", "us-east-1"),
            definitions_dir=e.get("HAREL_DEFINITIONS_DIR"),
            worker_id=e.get("HAREL_WORKER_ID"),
            visibility=float(e.get("HAREL_VISIBILITY", "30")),
            concurrency=int(e.get("HAREL_CONCURRENCY", "256")),
            tui_interval_ms=int(e.get("HAREL_TUI_INTERVAL_MS", "1000")),
            tui_theme=e.get("HAREL_TUI_THEME", "nord"),
            trace=e.get("HAREL_TRACE", "").lower() in ("1", "true", "yes", "on"),
            trace_max=int(e.get("HAREL_TRACE_MAX", "200")),
            prefix=e.get("HAREL_PREFIX", "harel"),
            create_schema=e.get("HAREL_CREATE_SCHEMA", "1").lower() not in ("0", "false", "no", "off"),
        )

    def schema_kwargs(self) -> dict:
        """`prefix` and `create_schema` for a backend that has a schema (see
        `harel.engine.schema`)."""
        return {"prefix": self.prefix, "create_schema": self.create_schema}

    def libsql_kwargs(self) -> dict:
        """Connection kwargs for the libSQL store/transport: an embedded replica (`sync_url` +
        `auth_token`) when `HAREL_LIBSQL_SYNC_URL` is set, else a plain local file."""
        if self.libsql_sync_url:
            return {"sync_url": self.libsql_sync_url, "auth_token": self.libsql_auth_token}
        return {}


def require(value: Optional[str], var: str) -> str:
    """Return `value`, or raise if the variable a selected backend needs was not set."""
    if not value:
        raise ValueError(f"{var} is required for the selected backend")
    return value
