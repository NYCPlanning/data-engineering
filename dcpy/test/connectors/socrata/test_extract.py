from unittest import mock

from dcpy.models.connectors import socrata
from dcpy.connectors.socrata import extract

from dcpy.test.conftest import mock_request_get


@mock.patch("requests.get", side_effect=mock_request_get)
def test_get_version_from_socrata(mock_request_get):
    test_set = extract.Source(
        type="socrata",
        org=socrata.Org.nyc,
        uid="w7w3-xahh",
        format="csv",
    )
    version = extract.get_version(test_set)
    assert version == "20240412"
