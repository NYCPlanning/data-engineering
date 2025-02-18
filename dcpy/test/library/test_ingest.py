import os
from unittest.mock import patch
from sqlalchemy import text
import pytest

from dcpy.library.ingest import Ingestor
from dcpy.test.conftest import mock_request_get

from . import (
    pg,
    recipe_engine,
    get_config_file,
    TEST_DATASET_NAME,
    TEST_DATASET_VERSION,
    TEST_DATASET_CONFIG_FILE,
    TEST_DATASET_OUTPUT_PATH,
    template_path,
)


@pytest.fixture
def test_dataset():
    

def test_format_field_names():
    assert False


def test_ingest_postgres():
    ingestor = Ingestor()
    ingestor.postgres(TEST_DATASET_CONFIG_FILE, postgres_url=recipe_engine)
    with pg.connect() as conn:
        for version in [TEST_DATASET_VERSION, "latest"]:
            sql = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_schema = '{TEST_DATASET_NAME}'
                AND    table_name   = '{version}'
            );
            """
            result = conn.execute(text(sql)).fetchall()
            assert result[0][0], (
                f"{TEST_DATASET_NAME}.{version} is not in postgres database yet"
            )
        conn.execute(text(f"DROP SCHEMA IF EXISTS {TEST_DATASET_NAME} CASCADE;"))


def test_ingest_csv():
    ingestor = Ingestor()
    ingestor.csv(TEST_DATASET_CONFIG_FILE, compress=True)
    assert os.path.isfile(f"{TEST_DATASET_OUTPUT_PATH}.csv")


def test_ingest_pgdump():
    ingestor = Ingestor()
    ingestor.pgdump(TEST_DATASET_CONFIG_FILE, compress=True)
    assert os.path.isfile(f"{TEST_DATASET_OUTPUT_PATH}.sql")


def test_ingest_geojson():
    ingestor = Ingestor()
    ingestor.geojson(TEST_DATASET_CONFIG_FILE, compress=True)
    assert os.path.isfile(f"{TEST_DATASET_OUTPUT_PATH}.geojson")


def test_ingest_shapefile():
    ingestor = Ingestor()
    ingestor.shapefile(TEST_DATASET_CONFIG_FILE)
    assert os.path.isfile(f"{TEST_DATASET_OUTPUT_PATH}.shp.zip")


def test_ingest_version_overwrite():
    version_overwrite = "test_version"
    ingestor = Ingestor()
    ingestor.csv(TEST_DATASET_CONFIG_FILE, version=version_overwrite)
    assert os.path.isfile(
        f".library/datasets/{TEST_DATASET_NAME}/{version_overwrite}/{TEST_DATASET_NAME}.csv"
    )


@patch("requests.get", side_effect=mock_request_get)
def test_ingest_with_sql(request_get):
    ingestor = Ingestor()
    ingestor.csv(get_config_file("bpl_libraries_sql"))


@patch("requests.get", side_effect=mock_request_get)
def test_script(request_get):
    ingestor = Ingestor()
    ingestor.csv(f"{template_path}/bpl_libraries.yml", version="test")
    assert os.path.isfile(".library/datasets/bpl_libraries/test/bpl_libraries.csv")
