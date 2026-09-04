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


def test_access_token_raises_on_error_result(monkeypatch):
    client = make_client(monkeypatch, {"error": "invalid_client"})

    with pytest.raises(PermissionError):
        client.access_token
