import pytest
from unittest import mock

from dcpy.models.connectors import socrata
from dcpy.connectors.socrata import extract, connector

from dcpy.test.conftest import mock_request_get

source = extract.Source(
    type="socrata",
    org=socrata.Org.nyc,
    uid="w7w3-xahh",
    format="csv",
)


@mock.patch("requests.get", side_effect=mock_request_get)
def test_get_version_from_socrata(mock_request_get):
    version = extract.get_version(source)
    assert version == "20240412"


class TestConnector:
    connector = connector.SocrataConnector()

    @mock.patch("requests.get", side_effect=mock_request_get)
    def test_pull(self, get, tmp_path):
        file = tmp_path / "test.csv"
        self.connector.pull(
            key=source.uid, destination_path=file, **source.model_dump()
        )
        assert file.exists()

    @mock.patch("requests.get", side_effect=mock_request_get)
    def test_get_latest_version(self, get):
        assert (
            self.connector.get_latest_version(source.uid, **source.model_dump())
            == "20240412"
        )

    def test_push(self):
        with pytest.raises(NotImplementedError):
            self.connector.push(source.uid, **source.model_dump())
