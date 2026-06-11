"""Config.from_env — defaults, parsing, and the require() helper."""

import pytest

from harel.config import Config, require


def test_defaults_when_env_empty():
    cfg = Config.from_env({})
    assert cfg.store_backend == "sqlite"
    assert cfg.transport_backend == "redis"
    assert cfg.concurrency == 256
    assert cfg.visibility == 30.0
    assert cfg.mongo_db == "harel"
    assert cfg.sqs_queue == "stm.fifo"
    assert cfg.aws_region == "us-east-1"
    assert cfg.tui_interval_ms == 1000
    assert cfg.tui_theme == "nord"
    # backend-specific vars are None until set
    assert cfg.postgres_dsn is None and cfg.redis_url is None and cfg.libsql_db is None


def test_reads_and_coerces():
    cfg = Config.from_env(
        {
            "STM_STORE_BACKEND": "postgres",
            "STM_POSTGRES_DSN": "postgresql://x",
            "STM_CONCURRENCY": "8",
            "STM_VISIBILITY": "5",
            "STM_TUI_INTERVAL_MS": "500",
        }
    )
    assert cfg.store_backend == "postgres"
    assert cfg.postgres_dsn == "postgresql://x"
    assert cfg.concurrency == 8 and isinstance(cfg.concurrency, int)
    assert cfg.visibility == 5.0 and isinstance(cfg.visibility, float)
    assert cfg.tui_interval_ms == 500


def test_libsql_kwargs():
    assert Config.from_env({"STM_LIBSQL_DB": "x.db"}).libsql_kwargs() == {}
    cfg = Config.from_env(
        {"STM_LIBSQL_DB": "x.db", "STM_LIBSQL_SYNC_URL": "libsql://p", "STM_LIBSQL_AUTH_TOKEN": "t"}
    )
    assert cfg.libsql_kwargs() == {"sync_url": "libsql://p", "auth_token": "t"}


def test_require():
    assert require("v", "STM_X") == "v"
    with pytest.raises(ValueError, match="STM_X"):
        require(None, "STM_X")
    with pytest.raises(ValueError, match="STM_X"):
        require("", "STM_X")


def test_from_env_reads_os_environ_at_call_time(monkeypatch):
    """Defaults to os.environ and re-reads each call (so monkeypatch after import works)."""
    monkeypatch.setenv("STM_STORE_BACKEND", "redis")
    assert Config.from_env().store_backend == "redis"
    monkeypatch.setenv("STM_STORE_BACKEND", "mongo")
    assert Config.from_env().store_backend == "mongo"
