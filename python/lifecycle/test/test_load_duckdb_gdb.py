"""GDB layers loaded into DuckDB (curve library and ST_Read paths)."""

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import LineString

from dcpy.lifecycle import data_loader
from dcpy.lifecycle.builds.models import InputDataset
from dcpy.utils import duckdb as duckdb_utils

PRODUCT_DIR = Path(__file__).resolve().parents[3] / "products" / "cscl"


@pytest.fixture
def gpkg(tmp_path):
    path = tmp_path / "src.gpkg"
    gpd.GeoDataFrame(
        {"OBJECTID": [30, 10, 20], "name": ["c", "a", "b"]},
        geometry=[LineString([(0, 0), (i + 1, 1)]) for i in range(3)],
        crs="EPSG:2263",
    ).to_file(path, layer="lyr", driver="GPKG")
    return path


@pytest.fixture
def client(tmp_path):
    c = duckdb_utils.DuckDBClient(tmp_path / "t.duckdb", "s")
    yield c
    c.close()


def _ds(engine: str) -> InputDataset:
    return InputDataset(
        name="gdb",
        import_as="tbl",
        file_type="gdb",
        version="v1",
        load_engine=engine,
        custom={"layer_name": "lyr"},
    )


def test_curves_engine_loads_linear_and_arcs_tables(gpkg, client):
    data_loader.load_dataset_into_duckdb(_ds("curves"), client, gpkg)
    cols = [r[0] for r in client.conn.execute("DESCRIBE s.tbl").fetchall()]
    assert cols[0] == "ogc_fid" and "geom" in cols and "data_library_version" in cols
    assert client.conn.execute("SELECT ogc_fid FROM s.tbl").fetchall() == [
        (1,),
        (2,),
        (3,),
    ]
    arcs_cols = [r[0] for r in client.conn.execute("DESCRIBE s.tbl_arcs").fetchall()]
    assert arcs_cols[0] == "ogc_fid" and "data_library_version" not in arcs_cols


def test_gdal_engine_numbers_ogc_fid_by_objectid(gpkg, client):
    data_loader.load_dataset_into_duckdb(_ds("gdal"), client, gpkg)
    rows = client.conn.execute("SELECT ogc_fid, name FROM s.tbl ORDER BY 1").fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]
    cols = [r[0].lower() for r in client.conn.execute("DESCRIBE s.tbl").fetchall()]
    assert "objectid" not in cols and "geom" in cols


def test_unknown_gdb_engine_raises(gpkg, client):
    with pytest.raises(Exception, match="Unsupported load_engine"):
        data_loader.load_dataset_into_duckdb(_ds("pandas"), client, gpkg)


def test_gdb_layer_name_required(gpkg, client):
    ds = _ds("gdal")
    ds.custom = {}
    with pytest.raises(Exception, match="layer_name"):
        data_loader.load_dataset_into_duckdb(ds, client, gpkg)


def _merged(path: Path):
    from dcpy.lifecycle.builds.plan import _merge_base_recipe_inputs, recipe_from_yaml

    recipe = recipe_from_yaml(path, vars={})
    _merge_base_recipe_inputs(recipe, path, {})
    return recipe


def test_cscl_duckdb_recipe_inherits_every_postgres_input():
    pg = _merged(PRODUCT_DIR / "recipe.yml")
    duck = _merged(PRODUCT_DIR / "recipe_duckdb.yml")
    assert [
        (d.id, d.import_as) for d in duck.inputs.datasets if d.load_engine == "curves"
    ] and sum(d.load_engine == "curves" for d in duck.inputs.datasets) == 11
    assert {(d.id, d.import_as) for d in duck.inputs.datasets} == {
        (d.id, d.import_as) for d in pg.inputs.datasets
    }
    for d in duck.inputs.datasets:
        assert d.destination == "duckdb"
        if d.file_type == "gdb":
            assert d.load_engine in ("curves", "gdal")


def test_base_recipe_without_own_defaults_keeps_base_defaults():
    pluto = PRODUCT_DIR.parent / "pluto"
    minor = _merged(pluto / "recipe-minor.yml")
    base = _merged(pluto / "recipe.yml")
    by_key = {(d.id, d.import_as): d for d in minor.inputs.datasets}
    for d in base.inputs.datasets:
        if (d.id, d.import_as) in by_key and d.version_env_var is None:
            assert by_key[(d.id, d.import_as)].destination == d.destination
            assert by_key[(d.id, d.import_as)].load_engine == d.load_engine


def test_gdal_engine_falls_back_to_curve_library_on_curved_layers(
    gpkg, client, monkeypatch
):
    import duckdb

    def boom(*a, **k):
        raise duckdb.InvalidInputException("Unsupported geometry type in WKB")

    monkeypatch.setattr(client, "load_gdb_layer", boom)
    data_loader.load_dataset_into_duckdb(_ds("gdal"), client, gpkg)
    assert (
        client.get_table_count("tbl") == 3 and client.get_table_count("tbl_arcs") == 0
    )


def test_curves_engine_handles_empty_layer(tmp_path, client):
    gpd.GeoDataFrame(
        {"OBJECTID": [], "name": []}, geometry=[], crs="EPSG:2263"
    ).to_file(tmp_path / "e.gpkg", layer="lyr", driver="GPKG")
    data_loader.load_dataset_into_duckdb(_ds("curves"), client, tmp_path / "e.gpkg")
    assert (
        client.get_table_count("tbl") == 0 and client.get_table_count("tbl_arcs") == 0
    )


def test_gdal_engine_loads_attribute_only_table(tmp_path, client):
    import pandas as pd

    path = tmp_path / "t.gpkg"
    pd.DataFrame({"name": ["a", "b"]}).pipe(
        lambda df: gpd.GeoDataFrame(df, geometry=[None, None])
    ).to_file(path, layer="lyr", driver="GPKG")
    ds = _ds("gdal")
    ds.custom = {"layer_name": "lyr"}
    data_loader.load_dataset_into_duckdb(ds, client, path)
    assert client.conn.execute(
        "SELECT ogc_fid, name FROM s.tbl ORDER BY 1"
    ).fetchall() == [(1, "a"), (2, "b")]


@pytest.mark.parametrize("engine", ["curves", "gdal"])
def test_column_names_are_lowercased_like_ogr2ogr(gpkg, client, engine):
    data_loader.load_dataset_into_duckdb(_ds(engine), client, gpkg)
    cols = [r[0] for r in client.conn.execute("DESCRIBE s.tbl").fetchall()]
    assert cols == [c.lower() for c in cols] and "name" in cols
