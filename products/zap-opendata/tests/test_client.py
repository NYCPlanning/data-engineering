import pytest
from src import client as client_module
from src.client import Client


class StubApp:
    def __init__(self, result: dict):
        self.result = result

    def acquire_token_for_client(self, scopes):
        return self.result


def make_client(monkeypatch, result: dict) -> Client:
    monkeypatch.setattr(
        client_module.msal,
        "ConfidentialClientApplication",
        lambda client_id, authority=None, client_credential=None: StubApp(result),
    )
    return Client(
        zap_domain="https://crm.example",
        tenant_id="tenant",
        client_id="client",
        secret="secret",
    )


def test_request_header_carries_the_bearer_token(monkeypatch):
    client = make_client(monkeypatch, {"access_token": "abc123"})

    assert client.request_header == {"Authorization": "Bearer abc123"}


# What msal actually hands back on a bad secret. The extra keys are the point: an
# exact key-list match against ["error"] falls through this and dies on the
# missing "access_token" instead.
MSAL_ERROR_RESULT = {
    "error": "invalid_client",
    "error_description": "AADSTS7000215: Invalid client secret provided.",
    "error_codes": [7000215],
    "timestamp": "2026-09-05 12:00:00Z",
    "trace_id": "00000000-0000-0000-0000-000000000000",
    "correlation_id": "11111111-1111-1111-1111-111111111111",
}


@pytest.mark.parametrize(
    "result",
    [MSAL_ERROR_RESULT, {"error": "invalid_client"}],
    ids=["realistic_payload", "error_key_only"],
)
def test_access_token_raises_on_error_result(monkeypatch, result):
    client = make_client(monkeypatch, result)

    with pytest.raises(PermissionError):
        client.access_token
