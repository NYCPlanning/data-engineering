import zipfile
from io import BytesIO
from unittest.mock import Mock

import geopandas as gpd
import pytest
from shapely.geometry import box

from dcpy.connectors import usfws_wetlands

WATERSHEDS = ["02030101", "02030102"]


def _head(last_modified: str) -> Mock:
    return Mock(headers={"Last-Modified": last_modified}, raise_for_status=Mock())


def test_latest_version_is_newest_last_modified(monkeypatch):
    heads = {
        usfws_wetlands.zip_url("02030101"): _head("Thu, 01 Oct 2026 17:08:12 GMT"),
        usfws_wetlands.zip_url("02030102"): _head("Fri, 02 Oct 2026 09:00:00 GMT"),
    }
    monkeypatch.setattr(usfws_wetlands.requests, "head", lambda url, **_: heads[url])

    version = usfws_wetlands.USFWSWetlandsConnector().get_latest_version(
        "usfws_nyc_wetlands", watersheds=WATERSHEDS
    )

    assert version == "20261002"


def _zip_bytes() -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("placeholder.txt", "")
    return buffer.getvalue()


@pytest.fixture
def mock_downloads(monkeypatch):
    response = Mock(content=_zip_bytes(), raise_for_status=Mock())
    monkeypatch.setattr(usfws_wetlands.requests, "get", lambda url, **_: response)

    read_calls = []

    def read_file(path, layer):
        read_calls.append((path, layer))
        return gpd.GeoDataFrame(
            {"ATTRIBUTE": [layer] * 2},
            geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)],
            crs="EPSG:5070",
        )

    monkeypatch.setattr(gpd, "read_file", read_file)
    return read_calls


def test_pull_concatenates_each_watersheds_wetlands_layer(tmp_path, mock_downloads):
    result = usfws_wetlands.USFWSWetlandsConnector().pull(
        "usfws_nyc_wetlands", tmp_path, watersheds=WATERSHEDS
    )

    assert mock_downloads == [
        (
            tmp_path / ws / f"HU8_{ws}_Watershed" / f"HU8_{ws}.gdb",
            f"HU8_{ws}_Wetlands",
        )
        for ws in WATERSHEDS
    ]
    df = gpd.read_parquet(result["path"])
    assert len(df) == 4
    assert df.crs == "EPSG:5070"
