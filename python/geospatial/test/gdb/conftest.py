from pathlib import Path

import geopandas as gpd
import pytest

RESOURCES = Path(__file__).parent / "resources"

GDB_ZIP = "districts_sample.gdb.zip"

# nynta2010: two NYC neighborhood tabulation areas, chosen for contrast - BK95
# (Erasmus) is a single, 58-vertex polygon, while BK99 (park-cemetery-etc-
# Brooklyn) is a genuinely pathological multi-part polygon: 37 parts, ~14,600
# vertices. The point of including BK99 is to make sure row-level diff
# counting is driven by the key column, not by geometry complexity - a
# comparison shouldn't get slower, wronger, or crash just because one row's
# shape is enormous.
NTA_LAYER = "nynta2010"
NTA_KEY = ["NTACode"]
NTA_INSANE_KEY = "BK99"
NTA_SIMPLE_KEY = "BK95"

# nyad: two NY State Assembly districts, picked as the simplest (lowest
# vertex-count) polygons in the source gdb.
AD_LAYER = "nyad"
AD_KEY = ["AssemDist"]


@pytest.fixture(scope="function")
def utils_resources_path():
    return RESOURCES


@pytest.fixture
def nta_gdf(utils_resources_path) -> gpd.GeoDataFrame:
    return gpd.read_file(utils_resources_path / GDB_ZIP, layer=NTA_LAYER)


@pytest.fixture
def ad_gdf(utils_resources_path) -> gpd.GeoDataFrame:
    return gpd.read_file(utils_resources_path / GDB_ZIP, layer=AD_LAYER)
