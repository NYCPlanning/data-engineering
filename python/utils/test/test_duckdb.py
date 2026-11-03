import pytest

from dcpy.utils import duckdb as duckdb_utils


def _setting(client: duckdb_utils.DuckDBClient, name: str):
    return client.conn.execute(f"SELECT current_setting('{name}')").fetchone()[0]  # type: ignore[index]


@pytest.fixture
def no_duckdb_env(monkeypatch):
    monkeypatch.delenv("DUCKDB_MEMORY_LIMIT", raising=False)
    monkeypatch.delenv("DUCKDB_THREADS", raising=False)


def test_client_honours_memory_and_thread_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "1GB")
    monkeypatch.setenv("DUCKDB_THREADS", "1")
    client = duckdb_utils.DuckDBClient(db_path=tmp_path / "x.duckdb", schema="s")
    # DuckDB reports memory_limit in binary units: 1GB == 953.6 MiB
    assert _setting(client, "memory_limit") == "953.6 MiB"
    assert _setting(client, "threads") == 1


def test_client_uses_duckdb_defaults_without_env_vars(tmp_path, no_duckdb_env):
    client = duckdb_utils.DuckDBClient(db_path=tmp_path / "x.duckdb", schema="s")
    default = duckdb_utils.duckdb.connect()
    for name in ("memory_limit", "threads"):
        expected = default.execute(f"SELECT current_setting('{name}')").fetchone()[0]  # type: ignore[index]
        assert _setting(client, name) == expected
