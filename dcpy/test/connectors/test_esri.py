import pytest
from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

from dcpy.models.connectors.esri import (
    FeatureServer,
    FeatureServerLayer,
    Server,
    servers,
)
from dcpy.connectors.esri import arcgis_feature_service as arcfs
from dcpy.test.conftest import mock_request_get

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
ERROR_DATASET = "error"


@patch("requests.get", side_effect=mock_request_get)
class TestFeatureServer(TestCase):
    def test_get_feature_server_layers(self, request_get):
        layers = arcfs.get_feature_server_layers(MULTIPLE_LAYER_FS)
        assert len(layers) == 2
        assert layers[0] == MULTIPLE_LAYER


def test_label():
    assert MULTIPLE_LAYER.layer_label == LAYER_LABEL


@patch("requests.get", side_effect=mock_request_get)
class TestResolveLayer(TestCase):
    def test_explicit_id_and_name(self, request_get):
        layer = arcfs.resolve_layer(
            MULTIPLE_LAYER_FS, layer_id=LAYER_ID, layer_name=LAYER_NAME
        )
        assert layer == MULTIPLE_LAYER

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
