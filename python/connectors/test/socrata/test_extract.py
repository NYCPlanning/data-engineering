from unittest import mock

import pytest
import requests

from dcpy.connectors.socrata import configuration, connector, extract

from ..conftest import mock_request_get

ORG = configuration.Org.nyc
UID = "w7w3-xahh"
FORMAT: configuration.ValidFormat = "csv"


@mock.patch("requests.get", side_effect=mock_request_get)
def test_get_version_from_socrata(mock_request_get):
    version = extract.get_version(ORG, UID)
    assert version == "20240412"


class TestConnector:
    connector = connector.SocrataConnector()

    @mock.patch("requests.get", side_effect=mock_request_get)
    def test_pull(self, get, tmp_path):
        file = tmp_path / "test.csv"
        self.connector.pull(key=UID, destination_path=file, org=ORG, format=FORMAT)
        assert file.exists()

    @mock.patch("requests.get", side_effect=mock_request_get)
    def test_get_latest_version(self, get):
        assert self.connector.get_latest_version(UID, org=ORG) == "20240412"

    def test_push(self):
        with pytest.raises(NotImplementedError):
            self.connector.push(UID, version="asfd")


def _response(*chunks: bytes) -> mock.Mock:
    response = mock.Mock()
    response.iter_content.return_value = list(chunks)
    return response


def _count(n: int) -> mock.Mock:
    response = mock.Mock()
    response.json.return_value = [{"count": str(n)}]
    return response


@mock.patch.object(extract, "QUERY_PAGE_SIZE", 2)
@mock.patch("dcpy.connectors.socrata.extract._socrata_request")
def test_query_download_pages_into_one_csv(socrata_request, tmp_path):
    socrata_request.side_effect = [
        _count(3),
        _response(b"route_id\nB1\n", b"B2\n"),
        # the header can arrive split across chunks
        _response(b"route", b"_id\nB3\n"),
    ]

    path = tmp_path / "stops.csv"
    extract.download(ORG, UID, "csv", path, where="in_effect='true'")

    assert path.read_bytes() == b"route_id\nB1\nB2\nB3\n"
    first_page, second_page = socrata_request.call_args_list[1:]
    assert first_page.args[0] == f"https://data.cityofnewyork.us/resource/{UID}.csv"
    assert first_page.kwargs["params"] == {
        "$order": ":id",
        "$limit": 2,
        "$offset": 0,
        "$where": "in_effect='true'",
    }
    assert second_page.kwargs["params"]["$offset"] == 2


@mock.patch.object(extract, "QUERY_PAGE_SIZE", 2)
@mock.patch("dcpy.connectors.socrata.extract._socrata_request")
def test_query_download_retries_a_dropped_page_from_its_start(
    socrata_request, tmp_path
):
    dropped = mock.Mock()
    dropped.iter_content.return_value = iter_then_raise(b"route_id\nB3")
    socrata_request.side_effect = [
        _count(3),
        _response(b"route_id\nB1\nB2\n"),
        dropped,
        _response(b"route_id\nB3\n"),
    ]

    path = tmp_path / "stops.csv"
    extract.download(ORG, UID, "csv", path, endpoint="query")

    assert path.read_bytes() == b"route_id\nB1\nB2\nB3\n"
    assert "$where" not in socrata_request.call_args_list[1].kwargs["params"]


def iter_then_raise(chunk: bytes):
    yield chunk
    raise requests.exceptions.ChunkedEncodingError("connection broken")


@mock.patch("dcpy.connectors.socrata.extract._socrata_request")
def test_download_streams_the_export_to_disk(socrata_request, tmp_path):
    response = mock.Mock()
    response.iter_content.return_value = [b"route_id\n", b"B1\n", b"B2\n"]
    socrata_request.return_value = response

    path = tmp_path / "stops.csv"
    extract.download(ORG, UID, "csv", path)

    assert path.read_bytes() == b"route_id\nB1\nB2\n"
    assert socrata_request.call_args.args[0].endswith(f"/views/{UID}/rows.csv")
    assert socrata_request.call_args.kwargs["stream"] is True


def test_query_url_rejects_shapefile():
    with pytest.raises(Exception, match="Unsupported socrata query format"):
        extract.get_query_url(ORG, UID, "shapefile")
