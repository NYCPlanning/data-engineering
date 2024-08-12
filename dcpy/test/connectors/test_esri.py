import json
import pytest
from unittest import TestCase
from unittest.mock import MagicMock, patch

from dcpy.models.connectors.esri import (
    FeatureServer,
    FeatureServerLayer,
    Server,
)
from dcpy.connectors.esri import arcgis_feature_service as arcfs
from dcpy.test.conftest import mock_request_get, MockResponse

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
        dataset = "National_Register_Building_Listings"
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
