import shutil
import zipfile
from pathlib import Path

import pytest
import shapely
from pyogrio.raw import read_arrow
from pytest import fixture

from dcpy.geospatial.curves.read import gdb_xy_grid
from dcpy.geospatial.gdb import fgdb
from dcpy.geospatial.shapefile_metadata import Metadata

GDB_ZIP = "geodatabase.gdb.zip"
SPATIAL_LAYER = "mappluto_one_row"
TABLE_LAYER = "pluto_one_row"
METADATA_XML = "esri_metadata.xml"


@fixture
def temp_gdb_zip_path(utils_resources_path, tmp_path):
    shutil.copy2(
        src=utils_resources_path / GDB_ZIP,
        dst=tmp_path / GDB_ZIP,
    )
    assert zipfile.is_zipfile(tmp_path / GDB_ZIP), (
        f"'{GDB_ZIP}' should be a valid zip file"
    )
    return tmp_path / GDB_ZIP


@fixture
def temp_gdb_nonzipped_path(temp_gdb_zip_path, tmp_path):
    shutil.unpack_archive(filename=temp_gdb_zip_path, extract_dir=tmp_path)
    gdb_path = tmp_path / temp_gdb_zip_path.stem
    assert gdb_path.is_dir(), "Expected a gdb directory, but found none"
    return gdb_path


@fixture
def temp_gdb_nested_zip_path(temp_gdb_nonzipped_path, tmp_path):
    """A zip with the .gdb one folder deep instead of at the archive root -
    matching how real prod deliveries are packaged (e.g. a LION zip's .gdb
    lives at "lion/lion.gdb/...", not the zip root), unlike zip_gdb()'s own
    always-top-level output."""
    zip_path = tmp_path / "nested.gdb.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for file in temp_gdb_nonzipped_path.rglob("*"):
            z.write(
                file,
                arcname=Path("some_subfolder")
                / file.relative_to(temp_gdb_nonzipped_path.parent),
            )
    return zip_path


@fixture
def temp_gdb_zip_wrong_extension_path(temp_gdb_zip_path, tmp_path):
    """The same zip bytes, but under a filename that doesn't end in
    ".gdb.zip" - matching CSCL's real recipe.yml exports (e.g.
    "nyclion_26c.zip"). pyogrio's native zip-GDB handling only fires when the
    filename itself ends in ".gdb.zip" - confirmed empirically that identical
    zip content opens fine under one name and fails under the other."""
    new_path = tmp_path / "not_a_gdb_extension.zip"
    shutil.copy2(src=temp_gdb_zip_path, dst=new_path)
    return new_path


@fixture
def temp_metadata_object(utils_resources_path):
    xml_file = utils_resources_path / METADATA_XML
    xml_content = xml_file.read_text()
    assert xml_content != "", (
        f"Non-empty string expected, got: '{xml_content}' instead."
    )
    md_object = Metadata.from_xml(xml_content)
    return md_object


@fixture
def path_fixture(request):
    return request.getfixturevalue(request.param)


gdb_paths = pytest.mark.parametrize(
    "path_fixture",
    [
        pytest.param(
            "temp_gdb_zip_path",
            id="run_tests_on_zipped_gdb",
        ),
        pytest.param(
            "temp_gdb_nonzipped_path",
            id="run_tests_on_nonzipped_gdb",
        ),
    ],
    indirect=True,
)


@gdb_paths
def test_get_layers(path_fixture):
    layers = fgdb.get_layers(path_fixture)
    assert layers == [SPATIAL_LAYER, TABLE_LAYER]


@gdb_paths
def test_layer_geometry_types(path_fixture):
    types = fgdb.layer_geometry_types(path_fixture)
    assert types[SPATIAL_LAYER] == "MultiPolygon Z"
    assert types[TABLE_LAYER] is None


def test_layer_geometry_types_accepts_zip_path_directly(temp_gdb_zip_path):
    """No manual /vsizip/ resolution needed - pyogrio handles a zipped GDB
    natively, straight from the zip's own path."""
    types = fgdb.layer_geometry_types(temp_gdb_zip_path)
    assert set(types) == {SPATIAL_LAYER, TABLE_LAYER}


def test_layer_geometry_types_accepts_zip_not_named_gdb_zip(
    temp_gdb_zip_wrong_extension_path,
):
    """pyogrio's native zip-GDB auto-detection keys off the filename suffix
    (".gdb.zip"), not the contents - a real recipe.yml export like
    "nyclion_26c.zip" needs the same explicit-path fallback as a nested .gdb,
    even though its .gdb is at the archive root."""
    types = fgdb.layer_geometry_types(temp_gdb_zip_wrong_extension_path)
    assert set(types) == {SPATIAL_LAYER, TABLE_LAYER}


def test_layer_geometry_types_accepts_nested_gdb_in_zip(temp_gdb_nested_zip_path):
    """Real prod deliveries nest the .gdb inside a folder rather than at the
    zip root (pyogrio's own zip auto-detection only finds a top-level .gdb) -
    layer_geometry_types must fall back to locating it explicitly."""
    types = fgdb.layer_geometry_types(temp_gdb_nested_zip_path)
    assert set(types) == {SPATIAL_LAYER, TABLE_LAYER}


def test_zip_gdb_round_trips(temp_gdb_nonzipped_path, tmp_path):
    """Zip an unzipped .gdb directory, then confirm the archive opens the
    same way a normal GDB export does: readable directly (no manual
    extraction/vsizip path) and with every layer intact."""
    zip_path = tmp_path / "rezipped.gdb.zip"
    fgdb.zip_gdb(temp_gdb_nonzipped_path, zip_path)

    assert zipfile.is_zipfile(zip_path)
    assert fgdb.get_layers(zip_path) == [SPATIAL_LAYER, TABLE_LAYER]

    # The .gdb directory itself must be the top-level entry in the archive -
    # not its contents flattened, and not nested under tmp_path's full path.
    with zipfile.ZipFile(zip_path) as z:
        top_level_dirs = {name.split("/")[0] for name in z.namelist()}
    assert top_level_dirs == {temp_gdb_nonzipped_path.name}


def test_zip_gdb_can_overwrite_its_own_source_zip(temp_gdb_zip_path, tmp_path):
    """The in-place metadata-edit workflow unzips a GDB, edits it, then
    rezips over the SAME path it came from - zip_gdb must support that."""
    unzip_dir = tmp_path / "unzipped"
    shutil.unpack_archive(filename=temp_gdb_zip_path, extract_dir=unzip_dir)
    gdb_dir = unzip_dir / temp_gdb_zip_path.stem

    fgdb.zip_gdb(gdb_dir, temp_gdb_zip_path)

    assert fgdb.get_layers(temp_gdb_zip_path) == [SPATIAL_LAYER, TABLE_LAYER]


@gdb_paths
def test_resolve_layer_explicit_valid(path_fixture):
    assert fgdb.resolve_layer(path_fixture, SPATIAL_LAYER) == SPATIAL_LAYER


@gdb_paths
def test_resolve_layer_explicit_invalid(path_fixture):
    with pytest.raises(LookupError, match="nonexistent_layer"):
        fgdb.resolve_layer(path_fixture, "nonexistent_layer")


@gdb_paths
def test_resolve_layer_omitted_ambiguous(path_fixture):
    with pytest.raises(ValueError, match="layer must be specified"):
        fgdb.resolve_layer(path_fixture)


def test_resolve_layer_omitted_unambiguous(temp_gdb_zip_path, monkeypatch):
    monkeypatch.setattr(fgdb, "get_layers", lambda _: [SPATIAL_LAYER])
    assert fgdb.resolve_layer(temp_gdb_zip_path) == SPATIAL_LAYER


@gdb_paths
def test_read_metadata(path_fixture):
    md = fgdb.read_metadata(gdb=path_fixture, layer=SPATIAL_LAYER)

    element = "esri"
    assert hasattr(md, element), f"Expected element '{element}', but found none"

    assert md.esri.crea_date == "20260203"
    assert md.esri.crea_time == "10392600"


@gdb_paths
def test_write_metadata(path_fixture, temp_metadata_object):
    layers_before_md_write = fgdb.get_layers(path_fixture)
    fgdb.write_metadata(
        gdb=path_fixture,
        layer=SPATIAL_LAYER,
        metadata=temp_metadata_object,
        overwrite=True,
    )
    layers_after_md_write = fgdb.get_layers(path_fixture)

    md = fgdb.read_metadata(path_fixture, SPATIAL_LAYER)
    element = "esri"
    assert hasattr(md, element), f"Expected element '{element}', but found none"

    assert md.esri.crea_date == "19611215"
    assert md.esri.crea_time == "00000000"
    # confirm that no gdb layers were lost during md writing operations
    assert sorted(layers_before_md_write) == sorted(layers_after_md_write)


@gdb_paths
def test_metadata_exists(path_fixture):
    originally_md_exists = fgdb.metadata_exists(gdb=path_fixture, layer=SPATIAL_LAYER)
    # remove metadata
    fgdb.remove_metadata(
        gdb=path_fixture,
        layer=SPATIAL_LAYER,
    )
    md_exists_after_removal = fgdb.metadata_exists(
        gdb=path_fixture, layer=SPATIAL_LAYER
    )
    assert originally_md_exists is True, "Expected layer metadata but found none"
    assert md_exists_after_removal is False, (
        "Expected no layer metadata, but found some"
    )


@gdb_paths
def test_remove_metadata(path_fixture):
    layers_before_md_removal = fgdb.get_layers(path_fixture)
    fgdb.remove_metadata(
        gdb=path_fixture,
        layer=SPATIAL_LAYER,
    )
    layers_after_md_removal = fgdb.get_layers(path_fixture)

    md = fgdb.read_metadata(path_fixture, SPATIAL_LAYER)
    assert md is None
    # confirm that no gdb layers were lost during md removal
    assert sorted(layers_before_md_removal) == sorted(layers_after_md_removal)


def test_write_metadata_rewrites_layers_without_changing_data(
    temp_gdb_nonzipped_path, temp_metadata_object
):
    gdb = temp_gdb_nonzipped_path

    def snapshot():
        layers = {}
        for layer in fgdb.get_layers(gdb):
            meta, table = read_arrow(str(gdb), layer=layer)
            layers[layer] = {
                "table": table,
                "geometry_name": meta["geometry_name"],
                "crs": meta["crs"],
                "grid": gdb_xy_grid(gdb, layer),
                "xml": fgdb.read_metadata(gdb, layer),
                "needs_pro_3_2": fgdb._client_version(gdb, layer) >= (13, 2),
            }
        return layers

    before = snapshot()
    fgdb.write_metadata(gdb, SPATIAL_LAYER, temp_metadata_object, overwrite=True)
    after = snapshot()

    assert list(after) == list(before)
    assert after[SPATIAL_LAYER]["xml"].esri.crea_date == "19611215"
    assert after[TABLE_LAYER]["xml"] == before[TABLE_LAYER]["xml"]
    assert [before[layer]["needs_pro_3_2"] for layer in before] == [False, True]
    for layer in before:
        assert after[layer]["table"].schema.equals(before[layer]["table"].schema)
        assert after[layer]["geometry_name"] == before[layer]["geometry_name"]
        assert after[layer]["crs"] == before[layer]["crs"]
        assert after[layer]["needs_pro_3_2"] == before[layer]["needs_pro_3_2"]
        assert after[layer]["grid"].keys() == before[layer]["grid"].keys()
        for key, value in before[layer]["grid"].items():
            assert float(after[layer]["grid"][key]) == pytest.approx(float(value))
    # includes BigInteger and DateOnly fields
    assert after[TABLE_LAYER]["table"].equals(before[TABLE_LAYER]["table"])

    # GDAL writes the grid with 15 significant digits, which moves coordinates by
    # well under the grid's resolution
    def coords(layers, layer):
        geometry = layers[layer]["table"][layers[layer]["geometry_name"]]
        return shapely.get_coordinates(
            shapely.from_wkb(geometry.to_numpy(zero_copy_only=False)), include_z=True
        )

    resolution = 1 / float(before[SPATIAL_LAYER]["grid"]["XYSCALE"])
    shift = abs(coords(after, SPATIAL_LAYER) - coords(before, SPATIAL_LAYER)).max()
    assert shift < resolution / 100
    attributes = [
        c
        for c in before[SPATIAL_LAYER]["table"].column_names
        if c
        not in (before[SPATIAL_LAYER]["geometry_name"], "Shape_Length", "Shape_Area")
    ]
    assert (
        after[SPATIAL_LAYER]["table"]
        .select(attributes)
        .equals(before[SPATIAL_LAYER]["table"].select(attributes))
    )


def test_write_metadata_refuses_curved_layers(utils_resources_path, tmp_path):
    shutil.unpack_archive(utils_resources_path / "districts_sample.gdb.zip", tmp_path)
    (gdb,) = tmp_path.glob("*.gdb")
    with pytest.raises(ValueError, match="curved geometry"):
        fgdb.remove_metadata(gdb, fgdb.get_layers(gdb)[0])
