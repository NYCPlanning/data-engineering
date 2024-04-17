from pathlib import Path
import json

RESOURCES = Path(__file__).parent / "resources"
TEST_DATA_DIR = "test_data"
FAKE_VERSION = "v001"


def mock_request_get(url):
    class MockResponse:
        def __init__(self, content: bytes):
            self.content = content

        def json(self):
            return json.loads(self.content)

        def raise_for_status(self):
            pass

    test_files = {
        "https://www.bklynlibrary.org/locations/json": "bpl_libraries.json",
        "https://data.cityofnewyork.us/api/views/w7w3-xahh/rows.csv": "dca_operatingbusinesses.csv",
        "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/pad_24a.zip": "pad_24a.zip",
        "https://health.data.ny.gov/api/views/izta-vnpq/rows.csv": "nysdoh_nursinghomes.csv",
        "https://data.cityofnewyork.us/api/views/w7w3-xahh.json": "dca_operatingbusinesses_metadata.json",
    }

    if url not in test_files:
        raise Exception(f"Url {url} has not been configured with test data")

    with open(RESOURCES / "test_sources" / test_files[url], "rb") as file:
        return MockResponse(file.read())
