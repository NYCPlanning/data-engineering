import json
import os
from pathlib import Path

import pytest

# Must be set before dcpy.configuration is imported anywhere in this test run: it
# otherwise defaults BUILD_ENGINE_SCHEMA to the sanitized git branch name, which makes
# any test that reaches the build-schema fallback pass or fail depending on the branch
# it runs on.
os.environ["RECIPES_BUCKET"] = "test-recipes"
os.environ["PUBLISHING_BUCKET"] = "test-publishing"
os.environ["BUILD_ENGINE_SCHEMA"] = "unit_tests"

TEST_RESOURCES_PATH = Path(__file__).parent / "resources"


@pytest.fixture(scope="session", autouse=True)
def ensure_no_callouts():
    import socket

    def guard(*args, **kwargs):
        raise Exception("No internet allowed in the unit tests!")

    socket.socket = guard


class MockResponse:
    def __init__(self, content: bytes):
        self.content = content

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        pass


def mock_request_get(
    url: str, headers=None, auth=None, params: dict | None = None
) -> MockResponse:
    """
    Mocks calls to request.get

    To use, annotate test with
    @mock.patch("requests.get", side_effect=mock_request_get)
    If you do this, the first argument of the test MUST be a sort of "dummy" variable. This is
    meant more for if the mocked variable is a Class than a function. But if you attempt to use
    a fixture as the first argument to a function with this annotation, it will not actually be accessed.

    To use, add a new path and filename to the `test_files` dictionary and add the appropriate file
    (containing a plain string response) to resources/mocked_responses
    """

    test_files = {
        "https://www.bklynlibrary.org/locations/json": "bpl_libraries.json",
        "https://data.cityofnewyork.us/api/views/w7w3-xahh/rows.csv": "dca_operatingbusinesses.csv",
        "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/pad_24a.zip": "pad_24a.zip",
        "https://health.data.ny.gov/api/views/izta-vnpq/rows.csv": "nysdoh_nursinghomes.csv",
        "https://data.cityofnewyork.us/api/views/w7w3-xahh.json": "dca_operatingbusinesses_metadata.json",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/National_Register_Building_Listings/FeatureServer": "arcfs_metadata.json",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/National_Register_Building_Listings_Zero/FeatureServer": "arcfs_metadata_no_layers.json",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/National_Register_Building_Listings_Multiple/FeatureServer": "arcfs_metadata_multiple_layers.json",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/National_Register_Building_Listings/FeatureServer/13": "arcfs_layer_metadata.json",
    }

    error_urls = [
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/error/FeatureServer",
        "https://services.arcgis.com/1xFZPtKn1wKC6POA/ArcGIS/rest/services/error/FeatureServer/13",
    ]

    if url in test_files:
        with open(
            TEST_RESOURCES_PATH / "mocked_responses" / test_files[url], "rb"
        ) as file:
            return MockResponse(file.read())
    elif url in error_urls:
        return MockResponse(b'{"error": "fake api error"}')

    raise Exception(f"Url {url} has not been configured with test data")
