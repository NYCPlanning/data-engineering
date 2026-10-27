"""Writing a GeoParquet layer as a zipped shapefile."""

import zipfile
from pathlib import Path

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pytest
import shapely
from shapely.geometry import MultiPolygon, Polygon

from dcpy.geospatial.curves.recurve import curved_geo_schema
from dcpy.geospatial.shapefile_write import (
    esri_field_names,
    fgdb_field_names,
    write_shapefile_from_parquet,
)

SQUARE = MultiPolygon([Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])])


def test_esri_field_names():
    # the prod atomic polygons shapefile's names, cut from the gdb layer's
    assert esri_field_names(
        [
            "BOROUGH",
            "CENSUSBLOCK_2000",
            "CENSUSBLOCK_2000_SUFFIX",
            "CENSUSTRACT_2000",
            "CENSUSBLOCK_2010",
            "CENSUSTRACT_2010",
            "SHAPE_Length",
        ]
    ) == {
        "BOROUGH": "BOROUGH",
        "CENSUSBLOCK_2000": "CENSUSBLOC",
        "CENSUSBLOCK_2000_SUFFIX": "CENSUSBL_1",
        "CENSUSTRACT_2000": "CENSUSTRAC",
        "CENSUSBLOCK_2010": "CENSUSBL_2",
        "CENSUSTRACT_2010": "CENSUSTR_1",
        "SHAPE_Length": "SHAPE_Leng",
    }


def test_esri_field_names_collide_case_insensitively():
    assert esri_field_names(["abcdefghijK", "ABCDEFGHIJk"]) == {
        "abcdefghijK": "abcdefghij",
        "ABCDEFGHIJk": "ABCDEFGH_1",
    }


def test_fgdb_field_names():
    # as the prod nyap shapefile: FGDB shape fields last, ArcGIS's names
    assert list(
        fgdb_field_names(
            ["SHAPE_Length", "SHAPE_Area", "BOROUGH", "CENSUSTRACT_2000"]
        ).items()
    ) == [
        ("BOROUGH", "BOROUGH"),
        ("CENSUSTRACT_2000", "CENSUSTRAC"),
        ("SHAPE_Length", "Shape_Leng"),
        ("SHAPE_Area", "Shape_Area"),
    ]


def _districts(path: Path) -> Path:
    gpd.GeoDataFrame(
        {
            "SchoolDist": pa.array([1, 2], pa.int16()).to_pandas(),
            "BoroName": ["Bronx", "Queens"],
            "blank": [None, None],
            "SHAPE_Length": [4.0, 4.0],
        },
        geometry=[SQUARE, SQUARE],
        crs="EPSG:2263",
    ).to_parquet(path)
    return path


def test_write_shapefile_from_parquet(tmp_path: Path):
    src = _districts(tmp_path / "nysd.parquet")
    out = tmp_path / "out" / "nysd.zip"

    n = write_shapefile_from_parquet(
        src,
        out,
        field_names={"BoroName": "BoroName", "SchoolDist": "SchoolDist"},
        zip_folder="nysd_26c",
        metadata_xml="<metadata/>",
    )

    assert n == 2
    with zipfile.ZipFile(out) as zf:
        names = sorted(zf.namelist())
        assert zf.read("nysd_26c/nysd.shp.xml") == b"<metadata/>"
    assert {"nysd_26c/nysd.shp", "nysd_26c/nysd.dbf", "nysd_26c/nysd.prj"} <= set(names)
    shp = f"/vsizip/{out}/nysd_26c/nysd.shp"
    info = pyogrio.read_info(shp)
    # field order follows field_names; unlisted columns are dropped
    assert list(info["fields"]) == ["BoroName", "SchoolDist"]
    assert info["geometry_type"] == "Polygon"
    gdf = pyogrio.read_dataframe(shp)
    assert gdf.crs.to_epsg() == 2263
    assert gdf["SchoolDist"].tolist() == [1, 2]
    assert gdf.geometry.iloc[0].equals(SQUARE.geoms[0])


def test_default_field_names_and_null_columns(tmp_path: Path):
    out = tmp_path / "nysd.zip"
    write_shapefile_from_parquet(_districts(tmp_path / "nysd.parquet"), out)
    info = pyogrio.read_info(f"/vsizip/{out}/nysd.shp")
    assert list(info["fields"]) == ["SchoolDist", "BoroName", "blank", "SHAPE_Leng"]


def test_rejects_bad_field_names(tmp_path: Path):
    src = _districts(tmp_path / "nysd.parquet")
    with pytest.raises(ValueError, match="longer than 10"):
        write_shapefile_from_parquet(
            src, tmp_path / "a.zip", field_names={"SHAPE_Length": "SHAPE_Length"}
        )
    with pytest.raises(ValueError, match="Duplicate"):
        write_shapefile_from_parquet(
            src,
            tmp_path / "a.zip",
            field_names={"BoroName": "name", "SchoolDist": "NAME"},
        )
    with pytest.raises(ValueError, match="not in"):
        write_shapefile_from_parquet(
            src, tmp_path / "a.zip", field_names={"nope": "nope"}
        )


def test_curves_are_linearized(tmp_path: Path):
    # a half-disc, CurvePolygon(CompoundCurve(arc (0,0)-(1,1)-(2,0), line back)), as
    # ISO WKB in a re-curved file (shapely can't build curves)
    wkb = _curve_polygon_wkb()
    linear = tmp_path / "linear.parquet"
    gpd.GeoDataFrame({"id": [1]}, geometry=[SQUARE], crs="EPSG:2263").to_parquet(linear)
    schema, geom_col = curved_geo_schema(pq.ParquetFile(str(linear)))
    table = pa.table(
        {"id": pa.array([1], pa.int64()), geom_col: pa.array([wkb], pa.binary())}
    )
    path = tmp_path / "curved.parquet"
    pq.write_table(table.cast(schema), path)

    out = tmp_path / "half.zip"
    write_shapefile_from_parquet(path, out)
    gdf = pyogrio.read_dataframe(f"/vsizip/{out}/half.shp")
    geom = gdf.geometry.iloc[0]
    assert geom.geom_type in ("Polygon", "MultiPolygon")
    # many vertices along the arc, area close to the half-disc's (pi / 2)
    assert shapely.get_num_coordinates(geom) > 10
    assert abs(geom.area - 3.14159 / 2) < 0.01


def _curve_polygon_wkb() -> bytes:
    import struct

    def point(x, y):
        return struct.pack("<dd", x, y)

    arc = struct.pack("<BII", 1, 8, 3) + point(0, 0) + point(1, 1) + point(2, 0)
    line = struct.pack("<BII", 1, 2, 2) + point(2, 0) + point(0, 0)
    compound = struct.pack("<BII", 1, 9, 2) + arc + line
    return struct.pack("<BII", 1, 10, 1) + compound
