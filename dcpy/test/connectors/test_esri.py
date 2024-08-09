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

SINGLE_LAYER_DATASET = "National_Register_Building_Listings"
SINGLE_LAYER_NAME = "National Register Building Listings"
SINGLE_LAYER_ID = 13
SINGLE_LAYER_LABEL = "National Register Building Listings (13)"
SINGLE_LAYER_FS = FeatureServer(server=Server.nys_parks, name=SINGLE_LAYER_DATASET)
SINGLE_LAYER = FeatureServerLayer(
    server=Server.nys_parks,
    name=SINGLE_LAYER_DATASET,
    layer_name=SINGLE_LAYER_NAME,
    layer_id=SINGLE_LAYER_ID,
)

MULTIPLE_LAYER_DATASET = ""
ERROR_DATASET = "error"


@patch("requests.get", side_effect=mock_request_get)
class TestFeatureServer(TestCase):
    def test_get_feature_server_layers(self, request_get):
        layers = arcfs.get_feature_server_layers(SINGLE_LAYER_FS)
        assert len(layers) == 1
        assert layers[0] == SINGLE_LAYER


@patch("requests.get", side_effect=mock_request_get)
class TestFeatureServerLayer(TestCase):
    def test_label(self, request_get):
        assert SINGLE_LAYER.layer_label == SINGLE_LAYER_LABEL

    def test_resolve_layer_id_explicit_id(self, request_get):
        layer = arcfs.resolve_layer_id(SINGLE_LAYER_FS, layer_id=SINGLE_LAYER_ID)
        assert layer == SINGLE_LAYER

    def test_resolve_layer_id_explicit_layer_name(self, request_get):
        layer = arcfs.resolve_layer_id(SINGLE_LAYER_FS)
        assert layer == SINGLE_LAYER

    def test_resolve_layer_id_only_layer(self, request_get):
        layer = arcfs.resolve_layer_id(SINGLE_LAYER_FS)
        assert layer == SINGLE_LAYER
