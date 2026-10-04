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
    assert cfg.sqs_queue == "harel.fifo"
    assert cfg.aws_region == "us-east-1"
    assert cfg.tui_interval_ms == 1000
    assert cfg.tui_theme == "nord"
    # backend-specific vars are None until set
    assert cfg.postgres_dsn is None and cfg.redis_url is None and cfg.libsql_db is None


def test_reads_and_coerces():
    cfg = Config.from_env(
        {
            "HAREL_STORE_BACKEND": "postgres",
            "HAREL_POSTGRES_DSN": "postgresql://x",
            "HAREL_CONCURRENCY": "8",
            "HAREL_VISIBILITY": "5",
            "HAREL_TUI_INTERVAL_MS": "500",
        }
    )
    assert cfg.store_backend == "postgres"
    assert cfg.postgres_dsn == "postgresql://x"
    assert cfg.concurrency == 8 and isinstance(cfg.concurrency, int)
    assert cfg.visibility == 5.0 and isinstance(cfg.visibility, float)
    assert cfg.tui_interval_ms == 500


def test_libsql_kwargs():
    assert Config.from_env({"HAREL_LIBSQL_DB": "x.db"}).libsql_kwargs() == {}
    cfg = Config.from_env(
        {"HAREL_LIBSQL_DB": "x.db", "HAREL_LIBSQL_SYNC_URL": "libsql://p", "HAREL_LIBSQL_AUTH_TOKEN": "t"}
    )
    assert cfg.libsql_kwargs() == {"sync_url": "libsql://p", "auth_token": "t"}


def test_require():
    assert require("v", "HAREL_X") == "v"
    with pytest.raises(ValueError, match="HAREL_X"):
        require(None, "HAREL_X")
    with pytest.raises(ValueError, match="HAREL_X"):
        require("", "HAREL_X")


def test_from_env_reads_os_environ_at_call_time(monkeypatch):
    """Defaults to os.environ and re-reads each call (so monkeypatch after import works)."""
    monkeypatch.setenv("HAREL_STORE_BACKEND", "redis")
    assert Config.from_env().store_backend == "redis"
    monkeypatch.setenv("HAREL_STORE_BACKEND", "mongo")
    assert Config.from_env().store_backend == "mongo"


def test_a_variable_under_its_old_name_is_not_read_but_logged_once(caplog):
    cfg = Config.from_env({"STM_STORE_BACKEND": "postgres"})
    Config.from_env({"STM_STORE_BACKEND": "postgres"})
    assert cfg.store_backend == "sqlite"
    message = "STM_STORE_BACKEND is not read since harel 0.7: set HAREL_STORE_BACKEND instead"
    assert caplog.text.count(message) == 1
