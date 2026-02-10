import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest

from dcpy.connectors.esri import arcgis_feature_service as arcfs
from dcpy.models.connectors.esri import (
    FeatureServer,
    FeatureServerLayer,
    Server,
)
from dcpy.test.conftest import MockResponse, mock_request_get

DATASET_NAME = "National_Register_Building_Listings"
LAYER_NAME = "National Register Building Listings"
LAYER_ID = 13
LAYER_LABEL = "National Register Building Listings (13)"


MULTIPLE_LAYER_DATASET = "National_Register_Building_Listings_Multiple"
MULTIPLE_LAYER_FS = FeatureServer(server=Server.nys_parks, name=MULTIPLE_LAYER_DATASET)
MULTIPLE_LAYER = FeatureServerLayer(
    server=Server.nys_parks,
    name=MULTIPLE_LAYER_DATASET,
    layer_name=LAYER_NAME,
    layer_id=LAYER_ID,
)
ZERO_LAYER_DATASET = "National_Register_Building_Listings_Zero"


@patch("requests.get", side_effect=mock_request_get)
class TestFeatureServer(TestCase):
    def test_get_feature_server_layers(self, request_get):
        layers = arcfs.get_feature_server_layers(MULTIPLE_LAYER_FS)
        assert len(layers) == 2
        assert layers[0] == MULTIPLE_LAYER

    def test_get_metadata_error(self, request_get):
        with pytest.raises(Exception, match="Error fetching ESRI Server metadata"):
            arcfs.get_feature_server_metadata(
                FeatureServer(server=Server.nys_parks, name="error")
            )


def test_label():
    assert MULTIPLE_LAYER.layer_label == LAYER_LABEL


@patch("requests.get", side_effect=mock_request_get)
class TestResolveLayer(TestCase):
    def test_explicit_id_and_name(self, request_get):
        layer = arcfs.resolve_layer(
            MULTIPLE_LAYER_FS, layer_id=LAYER_ID, layer_name=LAYER_NAME
        )
        assert layer == MULTIPLE_LAYER

    def test_explicit_id_and_name_invalid(self, request_get):
        with pytest.raises(LookupError):
            arcfs.resolve_layer(MULTIPLE_LAYER_FS, layer_id=-1, layer_name=LAYER_NAME)

    def test_explicit_id(self, request_get):
        layer = arcfs.resolve_layer(MULTIPLE_LAYER_FS, layer_id=LAYER_ID)
        assert layer == MULTIPLE_LAYER

    def test_explicit_layer_name(self, request_get):
        layer = arcfs.resolve_layer(MULTIPLE_LAYER_FS, layer_name=LAYER_NAME)
        assert layer == MULTIPLE_LAYER

    def test_implicit_multiple(self, request_get):
        with pytest.raises(ValueError):
            arcfs.resolve_layer(MULTIPLE_LAYER_FS)

    def test_implicit_single(self, request_get):
        dataset = DATASET_NAME
        fs = FeatureServer(server=Server.nys_parks, name=dataset)
        layer = FeatureServerLayer(
            server=Server.nys_parks,
            name=dataset,
            layer_name=LAYER_NAME,
            layer_id=LAYER_ID,
        )
        resolved_layer = arcfs.resolve_layer(fs)
        assert resolved_layer == layer

    def test_implicit_no_layers(self, request_get):
        layer = FeatureServer(
            server=Server.nys_parks,
            name=ZERO_LAYER_DATASET,
        )
        with pytest.raises(LookupError, match="has no layers"):
            arcfs.resolve_layer(layer)

    def test_missing_id(self, request_get):
        with pytest.raises(LookupError):
            arcfs.resolve_layer(MULTIPLE_LAYER_FS, layer_id=0)

    def test_missing_name(self, request_get):
        with pytest.raises(LookupError):
            arcfs.resolve_layer(MULTIPLE_LAYER_FS, layer_name="Fake name")


def mock_query_layer(url: str, data: dict):
    features = [
        {"objectId": 1, "properties": {"Property": "value"}},
        {"objectId": 2, "properties": {"Property": "value"}},
        {"objectId": 3, "properties": {"Property": "value"}},
    ]
    if data.get("returnIdsOnly"):
        resp: dict = {"properties": {"objectIds": [1, 2, 3]}}
    else:
        if data.get("objectIds"):
            query_features = [f for f in features if f["objectId"] in data["objectIds"]]
        else:
            query_features = features
        resp = {"features": query_features}
    resp_bytes = json.dumps(resp).encode("utf-8")
    return MockResponse(resp_bytes)


@patch("requests.post", side_effect=mock_query_layer)
class TestGetLayer:
    @patch("requests.get", side_effect=mock_request_get)
    def test_get_layer_metadata_error(self, get, post):
        with pytest.raises(Exception, match="Error fetching ESRI Server metadata"):
            arcfs.get_layer_metadata(
                FeatureServerLayer(
                    server=Server.nys_parks, name="error", layer_name="", layer_id=13
                )
            )

    def test_get_layer(self, post: MagicMock):
        a = arcfs.get_layer(MULTIPLE_LAYER, crs=1)
        assert a["crs"] == 1
        assert len(a["features"]) == 3

        # one call to get ids, one call to get all data
        assert post.call_count == 2

    def test_get_layer_chunked(self, post: MagicMock):
        a = arcfs.get_layer(MULTIPLE_LAYER, crs=1, chunk_size=1)
        assert len(a["features"]) == 3

        # one call to get ids, three calls to get data
        assert post.call_count == 4


@patch("requests.get", side_effect=mock_request_get)
@patch("requests.post", side_effect=mock_query_layer)
def test_download_layer(get, post, tmp_path):
    filename = "does_not_exist.geojson"
    dataset = DATASET_NAME
    layer = FeatureServerLayer(
        server=Server.nys_parks,
        name=dataset,
        layer_name=LAYER_NAME,
        layer_id=LAYER_ID,
    )
    arcfs.download_layer(
        layer=layer,
        crs="EPSG:3857",
        path=tmp_path / filename,
    )
    print(tmp_path / filename)
    assert (tmp_path / filename).exists()


# the above tests handle specific cases
# the below are more just to make sure the connector class interacts with them properly
class TestConnector:
    connector = arcfs.ArcGISFeatureServiceConnector()
    conf = {
        "server": Server.nys_parks,
        "layer_name": LAYER_NAME,
        "layer_id": LAYER_ID,
    }

    @patch("requests.get", side_effect=mock_request_get)
    @patch("requests.post", side_effect=mock_query_layer)
    def test_pull(self, get, post, tmp_path):
        res = self.connector.pull(
            key=DATASET_NAME, destination_path=tmp_path, **self.conf
        )
        assert res["path"].exists()

    @patch("requests.get", side_effect=mock_request_get)
    @patch("requests.post", side_effect=mock_query_layer)
    def test_get_latest_version(self, get, post):
        assert (
            self.connector.get_latest_version(key=DATASET_NAME, **self.conf)
            == "20240806"
        )

    def test_push(self):
        with pytest.raises(NotImplementedError):
            self.connector.push(key=DATASET_NAME, **self.conf)
