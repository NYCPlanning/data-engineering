"""Tests for loading datasets into DuckDB."""

import pandas as pd
import pytest

from dcpy.connectors import ingest_datastore
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.lifecycle import data_loader
from dcpy.lifecycle.builds.models import InputDataset
from dcpy.lifecycle.connector_registry import connectors
from dcpy.utils import duckdb as duckdb_utils


@pytest.fixture
def test_data_dir(tmp_path):
    """Create test data directory with CSV and Parquet files."""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()

    # Create CSV dataset
    csv_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "value": [10.5, 20.3, 30.1, 40.8, 50.2],
        }
    )
    csv_path = data_dir / "test_dataset.csv"
    csv_df.to_csv(csv_path, index=False)

    # Create Parquet dataset
    parquet_df = pd.DataFrame(
        {
            "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
            "population": [8336817, 3979576, 2693976, 2320268, 1680992],
            "area_sq_mi": [302.6, 468.7, 227.3, 671.7, 517.9],
        }
    )
    parquet_path = data_dir / "test_cities.parquet"
    parquet_df.to_parquet(parquet_path, index=False)

    return data_dir, csv_df, parquet_df


@pytest.fixture
def setup_test_connectors(tmp_path):
    """Set up test connectors for loading data."""
    # Create a test connector using local storage
    test_storage = tmp_path / "test_storage"
    test_storage.mkdir()

    connector = ingest_datastore.Connector(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="test_storage",
            storage_backend=StorageType.LOCAL,
            local_dir=test_storage,
            _mkdir=True,
        )
    )
    connectors.register(connector, conn_type="test_source")

    yield test_storage

    # Cleanup - reset to default connectors
    from dcpy.lifecycle import connector_registry

    connector_registry._set_default_connectors()


def test_load_csv_into_duckdb(test_data_dir, tmp_path):
    """Test loading a CSV file into DuckDB."""
    data_dir, csv_df, _ = test_data_dir

    # Create DuckDB client
    db_path = tmp_path / "test.duckdb"
    duckdb_client = duckdb_utils.DuckDBClient(db_path=db_path, schema="test_schema")

    # Create InputDataset
    dataset = InputDataset(
        id="test_dataset",
        version="v1",
        source="test_source",
        file_type="csv",
        import_as="my_csv_table",
        custom={},
    )

    # Load the dataset
    csv_path = data_dir / "test_dataset.csv"
    table_name = data_loader.load_dataset_into_duckdb(
        ds=dataset,
        duckdb_client=duckdb_client,
        local_dataset_path=csv_path,
        include_version_col=True,
        include_ogc_fid_col=True,
    )

    # Verify table was created
    assert table_name == "test_schema.my_csv_table"
    assert duckdb_client.table_exists("my_csv_table")

    # Verify row count
    row_count = duckdb_client.get_table_count("my_csv_table")
    assert row_count == 5

    # Verify data was loaded correctly
    result_df = duckdb_client.query_to_df("SELECT * FROM test_schema.my_csv_table")
    assert len(result_df) == 5
    assert "ogc_fid" in result_df.columns
    assert "data_library_version" in result_df.columns
    assert result_df["data_library_version"].iloc[0] == "v1"

    # Verify data matches (excluding ogc_fid and version columns)
    data_cols = ["id", "name", "value"]
    assert list(result_df[data_cols].iloc[0]) == [1, "Alice", 10.5]

    duckdb_client.close()


def test_load_parquet_into_duckdb(test_data_dir, tmp_path):
    """Test loading a Parquet file into DuckDB."""
    data_dir, _, parquet_df = test_data_dir

    # Create DuckDB client
    db_path = tmp_path / "test.duckdb"
    duckdb_client = duckdb_utils.DuckDBClient(db_path=db_path, schema="test_schema")

    # Create InputDataset
    dataset = InputDataset(
        id="test_cities",
        version="20260101",
        source="test_source",
        file_type="parquet",
        import_as="cities",
        custom={},
    )

    # Load the dataset
    parquet_path = data_dir / "test_cities.parquet"
    table_name = data_loader.load_dataset_into_duckdb(
        ds=dataset,
        duckdb_client=duckdb_client,
        local_dataset_path=parquet_path,
        include_version_col=True,
        include_ogc_fid_col=True,
    )

    # Verify table was created
    assert table_name == "test_schema.cities"
    assert duckdb_client.table_exists("cities")

    # Verify row count
    row_count = duckdb_client.get_table_count("cities")
    assert row_count == 5

    # Verify data was loaded correctly
    result_df = duckdb_client.query_to_df("SELECT * FROM test_schema.cities")
    assert len(result_df) == 5
    assert "ogc_fid" in result_df.columns
    assert "data_library_version" in result_df.columns
    assert result_df["data_library_version"].iloc[0] == "20260101"

    # Verify data matches
    assert result_df["city"].tolist() == ["NYC", "LA", "Chicago", "Houston", "Phoenix"]

    duckdb_client.close()


def test_load_multiple_datasets_into_duckdb(test_data_dir, tmp_path):
    """Test loading both CSV and Parquet datasets into the same DuckDB database."""
    data_dir, csv_df, parquet_df = test_data_dir

    # Create DuckDB client
    db_path = tmp_path / "multi_dataset.duckdb"
    duckdb_client = duckdb_utils.DuckDBClient(db_path=db_path, schema="my_schema")

    # Load CSV dataset
    csv_dataset = InputDataset(
        id="test_dataset",
        version="v1",
        source="test_source",
        file_type="csv",
        import_as="people",
        custom={},
    )
    csv_path = data_dir / "test_dataset.csv"
    data_loader.load_dataset_into_duckdb(
        ds=csv_dataset,
        duckdb_client=duckdb_client,
        local_dataset_path=csv_path,
    )

    # Load Parquet dataset
    parquet_dataset = InputDataset(
        id="test_cities",
        version="20260101",
        source="test_source",
        file_type="parquet",
        import_as="cities",
        custom={},
    )
    parquet_path = data_dir / "test_cities.parquet"
    data_loader.load_dataset_into_duckdb(
        ds=parquet_dataset,
        duckdb_client=duckdb_client,
        local_dataset_path=parquet_path,
    )

    # Verify both tables exist
    assert duckdb_client.table_exists("people")
    assert duckdb_client.table_exists("cities")

    # Verify we can query both tables
    people_count = duckdb_client.get_table_count("people")
    cities_count = duckdb_client.get_table_count("cities")
    assert people_count == 5
    assert cities_count == 5

    # Verify we can join across tables (just to show they're in the same database)
    join_result = duckdb_client.query_to_df(
        """
        SELECT COUNT(*) as total_rows
        FROM my_schema.people
        CROSS JOIN my_schema.cities
        """
    )
    assert join_result["total_rows"].iloc[0] == 25  # 5 * 5

    duckdb_client.close()


def test_load_recipe_with_geospatial_data(setup_test_connectors, tmp_path):
    """Test loading a recipe with multiple datasets including geospatial Parquet into DuckDB."""
    import os

    import geopandas as gpd
    import yaml
    from shapely.geometry import Point  # type: ignore

    from dcpy.lifecycle.builds import load

    test_storage = setup_test_connectors

    # Create test datasets
    # 1. CSV dataset
    csv_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 30.1],
        }
    )
    csv_dir = test_storage / "test_csv" / "v1"
    csv_dir.mkdir(parents=True)
    csv_path = csv_dir / "test_csv.csv"  # Use dataset id as filename
    csv_df.to_csv(csv_path, index=False)

    # 2. Geospatial Parquet dataset with CRS (tests the storage version fix)
    gdf = gpd.GeoDataFrame(
        {
            "city": ["NYC", "LA", "Chicago"],
            "population": [8336817, 3979576, 2693976],
        },
        geometry=[
            Point(-74.0060, 40.7128),  # NYC
            Point(-118.2437, 34.0522),  # LA
            Point(-87.6298, 41.8781),  # Chicago
        ],
        crs="EPSG:4326",
    )
    parquet_dir = test_storage / "test_geo" / "v2"
    parquet_dir.mkdir(parents=True)
    parquet_path = parquet_dir / "test_geo.parquet"  # Use dataset id as filename
    gdf.to_parquet(parquet_path)

    # Create a recipe YAML
    recipe_dict = {
        "name": "Test Recipe",
        "product": "test_product",
        "version": "1.0",
        "inputs": {
            "dataset_defaults": {
                "destination": "duckdb",
                "file_type": "csv",
            },
            "datasets": [
                {
                    "name": "test_csv",
                    "id": "test_csv",
                    "version": "v1",
                    "source": "test_source",
                    "file_type": "csv",
                    "import_as": "people",
                },
                {
                    "name": "test_geo",
                    "id": "test_geo",
                    "version": "v2",
                    "source": "test_source",
                    "file_type": "parquet",
                    "import_as": "cities",
                },
            ],
        },
    }

    # Write recipe to temp file
    recipe_path = tmp_path / "recipe.lock.yml"
    with open(recipe_path, "w") as f:
        yaml.dump(recipe_dict, f)

    # Set BUILD_ENV_OUTPUT_DIR for this test
    output_dir = tmp_path / "build_output"
    output_dir.mkdir()
    old_env = os.environ.get("BUILD_ENV_OUTPUT_DIR")
    os.environ["BUILD_ENV_OUTPUT_DIR"] = str(output_dir)

    try:
        # Load the recipe using load_source_data_from_resolved_recipe
        load_result = load.load_source_data_from_resolved_recipe(
            recipe_path,
            clear_pg_schema=False,
            _write_metadata_file=False,
        )

        # Verify DuckDB file was created in correct location
        duckdb_path = output_dir / "test_product_1.0.duckdb"
        assert duckdb_path.exists(), f"DuckDB file should exist at {duckdb_path}"

        # Connect to the created DuckDB database and verify data
        import duckdb

        conn = duckdb.connect(str(duckdb_path))

        # Verify both tables exist
        tables = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
        ).fetchall()
        table_names = [(t[0], t[1]) for t in tables]
        assert ("dcas_lift_join", "people") in table_names
        assert ("dcas_lift_join", "cities") in table_names

        # Verify CSV data
        csv_result = conn.execute(
            "SELECT COUNT(*) FROM dcas_lift_join.people"
        ).fetchone()[0]
        assert csv_result == 3

        # Verify geospatial Parquet data loaded correctly (with CRS)
        geo_result = conn.execute(
            "SELECT COUNT(*) FROM dcas_lift_join.cities"
        ).fetchone()[0]
        assert geo_result == 3

        # Verify geometry column exists with CRS (this tests our storage version fix!)
        geo_cols = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'dcas_lift_join' AND table_name = 'cities'"
        ).fetchall()
        col_types = {col: dtype for col, dtype in geo_cols}
        assert "geometry" in col_types
        # The geometry type should include the SRID - this confirms v1.5.0+ storage is working
        assert "GEOMETRY" in col_types["geometry"]
        assert "EPSG:4326" in col_types["geometry"]

        # Verify we can query the geometry
        geom_check = conn.execute(
            "SELECT city, ST_AsText(geometry) as geom_wkt FROM dcas_lift_join.cities ORDER BY city"
        ).fetchall()
        assert len(geom_check) == 3
        assert geom_check[0][0] == "Chicago"  # First alphabetically
        assert "POINT" in geom_check[0][1]

        conn.close()

        # Verify load result
        assert load_result.name == "Test Recipe"
        assert "test_csv" in load_result.datasets
        assert "test_geo" in load_result.datasets

    finally:
        # Restore environment
        if old_env:
            os.environ["BUILD_ENV_OUTPUT_DIR"] = old_env
        elif "BUILD_ENV_OUTPUT_DIR" in os.environ:
            del os.environ["BUILD_ENV_OUTPUT_DIR"]


def test_load_without_ogc_fid(test_data_dir, tmp_path):
    """Test loading without ogc_fid column."""
    data_dir, csv_df, _ = test_data_dir

    db_path = tmp_path / "test.duckdb"
    duckdb_client = duckdb_utils.DuckDBClient(db_path=db_path, schema="test_schema")

    dataset = InputDataset(
        id="test_dataset",
        version="v1",
        source="test_source",
        file_type="csv",
        custom={},
    )

    csv_path = data_dir / "test_dataset.csv"
    data_loader.load_dataset_into_duckdb(
        ds=dataset,
        duckdb_client=duckdb_client,
        local_dataset_path=csv_path,
        include_ogc_fid_col=False,
        include_version_col=False,
    )

    result_df = duckdb_client.query_to_df("SELECT * FROM test_schema.test_dataset")
    assert "ogc_fid" not in result_df.columns
    assert "data_library_version" not in result_df.columns
    assert len(result_df) == 5

    duckdb_client.close()


def test_sanitize_table_names(tmp_path):
    """Test that table names are sanitized correctly."""
    db_path = tmp_path / "test.duckdb"
    duckdb_client = duckdb_utils.DuckDBClient(db_path=db_path, schema="Test-Schema")

    # Schema name should be sanitized
    assert duckdb_client.schema == "test_schema"

    # Create a simple CSV for testing
    test_df = pd.DataFrame({"col1": [1, 2, 3]})
    csv_path = tmp_path / "test.csv"
    test_df.to_csv(csv_path, index=False)

    # Test sanitization of table name with special characters
    dataset = InputDataset(
        id="my-test-table",
        version="v1",
        source="test_source",
        file_type="csv",
        import_as="My Table Name",
        custom={},
    )

    table_name = data_loader.load_dataset_into_duckdb(
        ds=dataset,
        duckdb_client=duckdb_client,
        local_dataset_path=csv_path,
        include_ogc_fid_col=False,
        include_version_col=False,
    )

    # Table name should be sanitized
    assert table_name == "test_schema.my_table_name"
    assert duckdb_client.table_exists("my_table_name")

    duckdb_client.close()


def test_load_specific_file_from_directory(tmp_path):
    """Test loading a specific file from a directory using custom.filename."""

    # Create a directory with multiple CSV files
    data_dir = tmp_path / "multi_csv_dataset"
    data_dir.mkdir()

    # Create multiple CSV files with different schemas to simulate real scenario
    df1 = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    df2 = pd.DataFrame({"project_id": [101, 102], "title": ["Project A", "Project B"]})
    df3 = pd.DataFrame({"other_col": [1, 2, 3]})

    (data_dir / "file1.csv").write_text(df1.to_csv(index=False))
    (data_dir / "projects.csv").write_text(df2.to_csv(index=False))
    (data_dir / "file3.csv").write_text(df3.to_csv(index=False))

    # Create DuckDB client
    db_path = tmp_path / "test.duckdb"
    duckdb_client = duckdb_utils.DuckDBClient(db_path=db_path, schema="test_schema")

    # Create InputDataset with custom.filename specified
    dataset = InputDataset(
        id="multi_csv",
        version="v1",
        source="test_source",
        file_type="csv",
        import_as="projects_table",
        custom={"filename": "projects.csv"},
    )

    # Load the dataset - passing the directory path
    table_name = data_loader.load_dataset_into_duckdb(
        ds=dataset,
        duckdb_client=duckdb_client,
        local_dataset_path=data_dir,  # Pass directory, not file
        include_ogc_fid_col=True,
        include_version_col=True,
    )

    # Verify table was created
    assert table_name == "test_schema.projects_table"
    assert duckdb_client.table_exists("projects_table")

    # Verify only the specified file was loaded
    result_df = duckdb_client.query_to_df("SELECT * FROM test_schema.projects_table")
    assert len(result_df) == 2
    assert "project_id" in result_df.columns
    assert "title" in result_df.columns
    assert result_df["title"].tolist() == ["Project A", "Project B"]

    # Verify no other files were loaded (should not have columns from other files)
    assert "name" not in result_df.columns
    assert "other_col" not in result_df.columns

    duckdb_client.close()
