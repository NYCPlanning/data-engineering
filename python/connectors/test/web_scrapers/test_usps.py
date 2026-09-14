import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

from dcpy.connectors.web_scrapers import usps
from dcpy.connectors.web_scrapers.usps import session_headers_from_curl

CHROME_CURL = """curl --url 'https://tools.usps.com/locations/getLocations' \\
  -H 'accept: application/json, text/javascript, */*; q=0.01' \\
  -b 'JSESSIONID=abc123; _ga=GA1.1.1; EntRegName=Jane||Doe' \\
  -H 'origin: https://tools.usps.com' \\
  -H 'sec-ch-ua-platform: "Android"' \\
  -H 'x-jfuguzwb-a: sensor-a-value' \\
  -H 'x-jfuguzwb-z: q' \\
  -H 'content-length: 99' \\
  -H 'user-agent: Mozilla/5.0 (Linux; Android 15; Pixel 9)' \\
  --data-raw '{"requestZipCode":"10034"}'"""


def test_keeps_the_whole_captured_header_set():
    """Partial replay is what pairs Android sensors with a macOS Firefox User-Agent."""
    assert session_headers_from_curl(CHROME_CURL) == {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "Cookie": "JSESSIONID=abc123; _ga=GA1.1.1; EntRegName=Jane||Doe",
        "origin": "https://tools.usps.com",
        "sec-ch-ua-platform": '"Android"',
        "x-jfuguzwb-a": "sensor-a-value",
        "x-jfuguzwb-z": "q",
        "user-agent": "Mozilla/5.0 (Linux; Android 15; Pixel 9)",
    }


def test_drops_headers_the_http_layer_owns():
    """A replayed Content-Length would be wrong for every body but the captured one."""
    assert "content-length" not in session_headers_from_curl(CHROME_CURL)


class TestHeaderMerge:
    def test_capture_overrides_static_defaults_across_casings(self):
        """A capture writes `user-agent`; STATIC_HEADERS writes `User-Agent`."""
        merged = usps._merge_headers(
            {"User-Agent": "firefox-macos", "Accept": "application/json"},
            {"user-agent": "chrome-android"},
        )
        assert merged == {"Accept": "application/json", "user-agent": "chrome-android"}

    def test_a_fresh_capture_replaces_the_hardcoded_user_agent(self):
        captured = session_headers_from_curl(CHROME_CURL)
        merged = usps._merge_headers(usps.STATIC_HEADERS, captured)
        agents = {v for k, v in merged.items() if k.lower() == "user-agent"}
        assert agents == {"Mozilla/5.0 (Linux; Android 15; Pixel 9)"}


def test_reads_cookie_supplied_as_a_header():
    curl = (
        "curl 'https://tools.usps.com/locations/getLocations' "
        "-H 'Cookie: JSESSIONID=abc123' -H 'x-jfuguzwb-a: sensor-a-value'"
    )
    assert session_headers_from_curl(curl)["Cookie"] == "JSESSIONID=abc123"


def test_values_containing_colons_are_kept_whole():
    curl = (
        "curl 'https://tools.usps.com/locations/getLocations' "
        "-H 'Cookie: JSESSIONID=0000ghXZ:1f7tset8a' -H 'x-jfuguzwb-a: a:b:c'"
    )
    headers = session_headers_from_curl(curl)
    assert headers["Cookie"] == "JSESSIONID=0000ghXZ:1f7tset8a"
    assert headers["x-jfuguzwb-a"] == "a:b:c"


def test_missing_cookie_raises():
    curl = "curl 'https://tools.usps.com/' -H 'x-jfuguzwb-a: sensor-a-value'"
    with pytest.raises(ValueError, match="No Cookie found"):
        session_headers_from_curl(curl)


def test_missing_sensor_headers_raises():
    """Without the sensors USPS serves a bot challenge, so catch it at capture time."""
    curl = "curl 'https://tools.usps.com/' -H 'Cookie: JSESSIONID=abc123'"
    with pytest.raises(ValueError, match="sensor headers"):
        session_headers_from_curl(curl)


class FakePost:
    """Stands in for requests.post; records the zips it was asked for."""

    def __init__(self):
        self.queried: list[str] = []

    def __call__(self, url, headers, json):
        zip_code = json["requestZipCode"]
        self.queried.append(zip_code)
        return SimpleNamespace(
            ok=True,
            status_code=200,
            text="[]",
            json=lambda: [{"locationID": f"loc-{zip_code}"}],
        )


class TestCheckpoint:
    connector = usps.USPSLocationsConnector()

    def test_absent_checkpoint_starts_from_scratch(self, tmp_path):
        assert self.connector._read_checkpoint(tmp_path / "nope.json") == ([], {})

    def test_round_trips(self, tmp_path):
        path = tmp_path / "checkpoint.json"
        locations = {"1": {"locationID": "1"}, "2": {"locationID": "2"}}
        self.connector._write_checkpoint(path, ["10001", "10002"], locations)
        assert self.connector._read_checkpoint(path) == (["10001", "10002"], locations)

    def test_write_is_atomic(self, tmp_path):
        """A killed process must not leave a truncated file that breaks the resume."""
        path = tmp_path / "checkpoint.json"
        self.connector._write_checkpoint(path, ["10001"], {"1": {"locationID": "1"}})
        assert list(tmp_path.iterdir()) == [path]

    def test_resumes_only_the_remaining_zips(self, tmp_path, monkeypatch):
        post = FakePost()
        monkeypatch.setattr(usps, "QUERY_ZIP_CODES", ["10001", "10002", "10003"])
        monkeypatch.setattr(usps.requests, "post", post)
        monkeypatch.setattr(usps.time, "sleep", lambda _: None)
        monkeypatch.setenv(self.connector.session_headers_env_var, '{"Cookie": "x"}')

        self.connector._write_checkpoint(
            tmp_path / self.connector.checkpoint_filename,
            ["10001"],
            {"loc-10001": {"locationID": "loc-10001"}},
        )
        result = self.connector.pull(key="", destination_path=tmp_path)

        assert post.queried == ["10002", "10003"]
        written = json.loads(result["path"].read_text())
        assert {loc["locationID"] for loc in written} == {
            "loc-10001",
            "loc-10002",
            "loc-10003",
        }

    def test_checkpoint_removed_once_complete(self, tmp_path, monkeypatch):
        monkeypatch.setattr(usps, "QUERY_ZIP_CODES", ["10001"])
        monkeypatch.setattr(usps.requests, "post", FakePost())
        monkeypatch.setenv(self.connector.session_headers_env_var, '{"Cookie": "x"}')

        self.connector.pull(key="", destination_path=tmp_path)
        assert not (tmp_path / self.connector.checkpoint_filename).exists()


class TestPacing:
    """A block costs a fresh browser capture, so the pacing is load-bearing, not taste."""

    connector = usps.USPSLocationsConnector()

    def test_waits_between_queries_but_not_before_the_first(
        self, tmp_path, monkeypatch
    ):
        slept = []
        monkeypatch.setattr(usps, "QUERY_ZIP_CODES", ["10001", "10002", "10003"])
        monkeypatch.setattr(usps.requests, "post", FakePost())
        monkeypatch.setattr(usps.time, "sleep", slept.append)
        monkeypatch.setenv(self.connector.session_headers_env_var, '{"Cookie": "x"}')
        self.connector.pull(key="", destination_path=tmp_path)
        assert slept == [usps.REQUEST_DELAY_SECONDS] * 2


class TestFailureDiagnostics:
    connector = usps.USPSLocationsConnector()

    def test_non_json_body_is_quoted_in_the_error(self):
        """Akamai's challenge and block pages are both HTML 200s; only the body differs."""
        body = "<html>This service is currently unavailable</html>"
        response = SimpleNamespace(
            ok=True,
            status_code=200,
            text=body,
            json=Mock(side_effect=requests.exceptions.JSONDecodeError("x", body, 0)),
        )
        with pytest.raises(RuntimeError, match="currently unavailable"):
            self.connector._parse_response(response, "10001", succeeded_so_far=1)
