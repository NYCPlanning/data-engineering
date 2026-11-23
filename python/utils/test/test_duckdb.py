from unittest.mock import MagicMock

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


@pytest.mark.parametrize(
    "endpoint, expected",
    [
        ("https://nyc3.digitaloceanspaces.com", ("nyc3.digitaloceanspaces.com", True)),
        ("https://gateway.example.test/", ("gateway.example.test", True)),
        ("http://localhost:7070", ("localhost:7070", False)),
        ("gateway.example.test", ("gateway.example.test", True)),
    ],
)
def test_s3_secret_endpoint(endpoint, expected):
    assert duckdb_utils._s3_secret_endpoint(endpoint) == expected


def test_setup_s3_secret_uses_path_style(monkeypatch):
    monkeypatch.setattr(duckdb_utils.configuration, "AWS_S3_ENDPOINT", "http://gw:7070")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    conn = MagicMock()

    duckdb_utils.setup_s3_secret(conn)

    secret_sql = conn.sql.call_args_list[-1].args[0]
    assert "ENDPOINT 'gw:7070'" in secret_sql
    assert "URL_STYLE 'path'" in secret_sql
    assert "USE_SSL false" in secret_sql
