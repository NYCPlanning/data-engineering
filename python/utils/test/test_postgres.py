import pytest

from dcpy.utils import postgres


@pytest.mark.parametrize(
    "test_inputs, expected_uri",
    [
        (
            ("local@local.local://person:pass", "db-1", "public"),
            "local@local.local://person:pass/db-1?options=--search_path%3Dpublic",
        ),
        (
            ("local@local.local://person:pass", "db-2", "pr_101"),
            "local@local.local://person:pass/db-2?options=--search_path%3Dpr_101,public",
        ),
    ],
)
def test_generate_engine_uri(test_inputs, expected_uri, monkeypatch):
    monkeypatch.delenv("PGOPTIONS", raising=False)
    actual_uri = postgres.generate_engine_uri(
        server_url=test_inputs[0],
        database=test_inputs[1],
        schema=test_inputs[2],
    )
    assert actual_uri == expected_uri


def test_generate_engine_uri_carries_pgoptions(monkeypatch):
    monkeypatch.setenv("PGOPTIONS", "-c jit=off -c default_transaction_read_only=off")
    assert postgres.generate_engine_uri("server", "db", "pr_101") == (
        "server/db?options=--search_path%3Dpr_101,public"
        "%20-c%20jit%3Doff%20-c%20default_transaction_read_only%3Doff"
    )
