# These are tests to be run after a full build
import pytest
from build_scripts import PG_CLIENT

from dcpy.configuration import BUILD_NAME

# TODO: publishing connector refactor - replace with: from dcpy.lifecycle.builds import builds
from dcpy.connectors.edm import publishing
from dcpy.connectors.edm.models import BuildKey

PRODUCT_S3_NAME = "db-template"
BUILD_KEY = BuildKey(product=PRODUCT_S3_NAME, build=str(BUILD_NAME))


# * test Transform stage
@pytest.mark.end_to_end()
def test_transform_staging():
    expected_build_tables = [
        "stg_nypl_libraries",
        "stg_bpl_libraries",
        "stg_qpl_libraries",
        "stg_dpr_parksproperties",
        "stg_dpr_greenthumb",
        "boroughs",
    ]
    actual_tables = PG_CLIENT.get_schema_tables()
    for table in expected_build_tables:
        assert table in actual_tables


# * test Export stage
@pytest.mark.end_to_end()
def test_export_build():
    # TODO: publishing connector refactor - replace with: builds.list_builds(product=BUILD_KEY.product)
    assert BUILD_KEY.build in publishing.get_builds(product=BUILD_KEY.product)


@pytest.mark.end_to_end()
def test_export_files():
    # Files are now organized in subdirectories:
    # - dataset_files/ contains the actual data files
    # - attachments/ contains data dictionaries and source_data_versions.csv
    # - diagnostics/dbt/ contains the dbt target directory
    # - Root contains metadata files
    expected_export_file_names = {
        # Root files
        "build_metadata.json",
        "recipe.lock.yml",
        "output.zip",
        # Attachments
        "attachments/source_data_versions.csv",
        "attachments/data_dictionary.yml",
        "attachments/data_dictionary.pdf",
        "attachments/data_dictionary.xlsx",
        # Dataset files
        "dataset_files/templatedb.csv",
        "dataset_files/templatedb.parquet",
        "dataset_files/templatedb_points.shp.zip",
        "dataset_files/templatedb_polygons.zip",
        "dataset_files/templatedb.zip",
    }
    # TODO: publishing connector refactor - replace with: builds.get_filenames(BUILD_KEY.product, BUILD_KEY.build)
    actual_files = publishing.get_filenames(BUILD_KEY)

    # Check for dbt diagnostics
    dbt_diagnostics_files = {
        f for f in actual_files if f.startswith("diagnostics/dbt/")
    }
    assert dbt_diagnostics_files, (
        "Expected dbt diagnostics directory to be present in export"
    )

    # Check that all expected files are present
    assert expected_export_file_names.issubset(actual_files), (
        f"Missing files: {expected_export_file_names - actual_files}"
    )
