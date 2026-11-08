from unittest import mock

import pytest

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


@mock.patch("dcpy.connectors.socrata.extract._socrata_request")
def test_download_with_where_queries_every_matching_row(socrata_request, tmp_path):
    count = mock.Mock()
    count.json.return_value = [{"count": "23218"}]
    rows = mock.Mock(content=b"route_id\nB1\n")
    socrata_request.side_effect = [count, rows]

    path = tmp_path / "stops.csv"
    extract.download(ORG, UID, "csv", path, where="in_effect='true'")

    assert path.read_bytes() == b"route_id\nB1\n"
    query_call = socrata_request.call_args_list[1]
    assert query_call.args[0] == f"https://data.cityofnewyork.us/resource/{UID}.csv"
    assert query_call.kwargs["params"] == {
        "$where": "in_effect='true'",
        "$limit": 23218,
    }


def test_query_url_rejects_shapefile():
    with pytest.raises(Exception, match="Unsupported socrata query format"):
        extract.get_query_url(ORG, UID, "shapefile")
