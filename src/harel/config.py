"""Centralised configuration — every `STM_*` environment variable, read in one place.

`Config.from_env()` parses the environment (names, defaults, int/float coercion) into a single
frozen object; the worker and the TUI build one and read attributes instead of scattering
`os.environ[...]` calls. It reads **at call time** (not import) so that callers/tests which set
env vars before invoking `build_*` still take effect — `from_env()` re-reads each time.

Backend-specific variables (e.g. `STM_POSTGRES_DSN`, only needed when the postgres backend is
selected) are `Optional` here; the builder validates the one it needs with `require()`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Optional


@dataclass(frozen=True)
class Config:
    """A snapshot of the `STM_*` environment. Build with `Config.from_env()`."""

    # --- backend selection ---
    store_backend: str = "sqlite"  # STM_STORE_BACKEND
    transport_backend: str = "redis"  # STM_TRANSPORT_BACKEND
    # --- redis ---
    redis_url: Optional[str] = None  # STM_REDIS_URL (transport, and store fallback)
    store_redis_url: Optional[str] = None  # STM_STORE_REDIS_URL (store; falls back to redis_url)
    # --- sqlite ---
    store_db: Optional[str] = None  # STM_STORE_DB
    transport_db: Optional[str] = None  # STM_TRANSPORT_DB
    # --- postgres ---
    postgres_dsn: Optional[str] = None  # STM_POSTGRES_DSN
    # --- rqlite ---
    rqlite_url: Optional[str] = None  # STM_RQLITE_URL
    # --- mongo ---
    mongo_url: Optional[str] = None  # STM_MONGO_URL
    mongo_db: str = "harel"  # STM_MONGO_DB
    # --- libsql (Turso) ---
    libsql_db: Optional[str] = None  # STM_LIBSQL_DB
    libsql_sync_url: Optional[str] = None  # STM_LIBSQL_SYNC_URL (embedded replica)
    libsql_auth_token: str = ""  # STM_LIBSQL_AUTH_TOKEN
    # --- aws (dynamodb store + sqs transport) ---
    sqs_endpoint: Optional[str] = None  # STM_SQS_ENDPOINT
    sqs_queue: str = "stm.fifo"  # STM_SQS_QUEUE
    dynamodb_endpoint: Optional[str] = None  # STM_DYNAMODB_ENDPOINT
    aws_region: str = "us-east-1"  # STM_AWS_REGION
    # --- worker loop ---
    definitions_dir: Optional[str] = None  # STM_DEFINITIONS_DIR
    worker_id: Optional[str] = None  # STM_WORKER_ID (None -> caller defaults to the hostname)
    visibility: float = 30.0  # STM_VISIBILITY (lease seconds)
    concurrency: int = 256  # STM_CONCURRENCY (events in flight on the async loop)
    # --- monitoring TUI ---
    tui_interval_ms: int = 1000  # STM_TUI_INTERVAL_MS (auto-refresh)
    tui_theme: str = "nord"  # STM_TUI_THEME

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "Config":
        """Read the `STM_*` variables from `env` (default `os.environ`) into a `Config`."""
        e = os.environ if env is None else env
        return cls(
            store_backend=e.get("STM_STORE_BACKEND", "sqlite"),
            transport_backend=e.get("STM_TRANSPORT_BACKEND", "redis"),
            redis_url=e.get("STM_REDIS_URL"),
            store_redis_url=e.get("STM_STORE_REDIS_URL"),
            store_db=e.get("STM_STORE_DB"),
            transport_db=e.get("STM_TRANSPORT_DB"),
            postgres_dsn=e.get("STM_POSTGRES_DSN"),
            rqlite_url=e.get("STM_RQLITE_URL"),
            mongo_url=e.get("STM_MONGO_URL"),
            mongo_db=e.get("STM_MONGO_DB", "harel"),
            libsql_db=e.get("STM_LIBSQL_DB"),
            libsql_sync_url=e.get("STM_LIBSQL_SYNC_URL"),
            libsql_auth_token=e.get("STM_LIBSQL_AUTH_TOKEN", ""),
            sqs_endpoint=e.get("STM_SQS_ENDPOINT"),
            sqs_queue=e.get("STM_SQS_QUEUE", "stm.fifo"),
            dynamodb_endpoint=e.get("STM_DYNAMODB_ENDPOINT"),
            aws_region=e.get("STM_AWS_REGION", "us-east-1"),
            definitions_dir=e.get("STM_DEFINITIONS_DIR"),
            worker_id=e.get("STM_WORKER_ID"),
            visibility=float(e.get("STM_VISIBILITY", "30")),
            concurrency=int(e.get("STM_CONCURRENCY", "256")),
            tui_interval_ms=int(e.get("STM_TUI_INTERVAL_MS", "1000")),
            tui_theme=e.get("STM_TUI_THEME", "nord"),
        )

    def libsql_kwargs(self) -> dict:
        """Connection kwargs for the libSQL store/transport: an embedded replica (`sync_url` +
        `auth_token`) when `STM_LIBSQL_SYNC_URL` is set, else a plain local file."""
        if self.libsql_sync_url:
            return {"sync_url": self.libsql_sync_url, "auth_token": self.libsql_auth_token}
        return {}


def require(value: Optional[str], var: str) -> str:
    """Return `value`, or raise if the variable a selected backend needs was not set."""
    if not value:
        raise ValueError(f"{var} is required for the selected backend")
    return value
