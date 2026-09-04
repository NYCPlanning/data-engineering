import json
from types import SimpleNamespace

import pytest
from src import runner as runner_module
from src.runner import Runner


class StubResponse:
    def __init__(self, payload: dict):
        self.payload = payload
        self.text = json.dumps(payload)

    def json(self) -> dict:
        return self.payload


@pytest.fixture
def runner(monkeypatch, tmp_path):
    """A Runner with the CRM client and database connection stubbed out."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        runner_module, "Client", lambda **kwargs: SimpleNamespace(request_header={})
    )
    monkeypatch.setattr(
        runner_module, "PG", lambda url, schema: SimpleNamespace(engine=None)
    )
    return Runner(name="dcp_projects", schema="test_download")


def stub_requests(monkeypatch, pages: list[dict]) -> list[str]:
    requested: list[str] = []

    def fake_get(url, headers=None):
        requested.append(url)
        return StubResponse(pages[len(requested) - 1])

    monkeypatch.setattr(runner_module.requests, "get", fake_get)
    return requested


def test_download_follows_odata_nextlink(runner, monkeypatch, tmp_path):
    pages = [
        {"value": [{"dcp_name": "2023K0001"}], "@odata.nextLink": "https://crm/page/1"},
        {"value": [{"dcp_name": "2023K0002"}], "@odata.nextLink": "https://crm/page/2"},
        {"value": [{"dcp_name": "2023K0003"}]},
    ]
    requested = stub_requests(monkeypatch, pages)

    runner.download()

    assert requested[1:] == ["https://crm/page/1", "https://crm/page/2"]
    written = sorted(p.name for p in (tmp_path / ".cache/dcp_projects").iterdir())
    assert written == [
        "dcp_projects_0.json",
        "dcp_projects_1.json",
        "dcp_projects_2.json",
    ]


def test_download_stops_on_last_page(runner, monkeypatch, tmp_path):
    requested = stub_requests(monkeypatch, [{"value": []}])

    runner.download()

    assert len(requested) == 1
    assert [p.name for p in (tmp_path / ".cache/dcp_projects").iterdir()] == [
        "dcp_projects_0.json"
    ]


def test_download_raises_on_error_payload(runner, monkeypatch):
    stub_requests(monkeypatch, [{"error": {"message": "no such entity"}}])

    with pytest.raises(FileNotFoundError):
        runner.download()


def test_download_requests_the_whole_table(runner, monkeypatch):
    requested = stub_requests(monkeypatch, [{"value": []}])

    runner.download()

    assert requested[0].endswith("/dcp_projects")
