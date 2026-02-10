from unittest import mock

import pytest

from dcpy.connectors.socrata import configuration, connector, extract
from dcpy.test.conftest import mock_request_get

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
