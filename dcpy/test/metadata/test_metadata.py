from pathlib import Path
from dcpy.metadata.models import Metadata as md

METADATA_PATH = Path(__file__).parent.resolve() / "resources" / "template_md.yml"
assert METADATA_PATH.exists()

metadata = md.from_yaml(METADATA_PATH)


def test_socrata_column_overrides_wkb_geom():
    dataset = metadata.dataset_package.get_dataset("primary_shapefile")
    col_names = {c.name for c in dataset.get_columns(metadata)}

    assert (
        "the_geom" in col_names
    ), """expected a column named `the_geom`.
    It should have overridden the existing column named wkb_geometry"""

    # Shapefile
    socrata_dest_cols_shp = metadata.get_destination(
        "socrata_shapefile"
    ).destination_column_metadata(metadata)
    socrata_dest_col_names_shp = {c.api_name for c in socrata_dest_cols_shp}
    assert (
        "the_geom_socrata" in socrata_dest_col_names_shp
    ), "The shapefile name should have been transformed to the socrata destination name"

    assert "omit_me_from_shapefile" not in socrata_dest_col_names_shp

    # CSV
    socrata_dest_cols_csv = metadata.get_destination(
        "socrata_csv"
    ).destination_column_metadata(metadata)
    socrata_dest_col_names_csv = {c.api_name for c in socrata_dest_cols_csv}

    assert "omit_me_from_shapefile" in socrata_dest_col_names_csv
