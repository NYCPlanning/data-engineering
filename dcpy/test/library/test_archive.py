import os

from sqlalchemy import text

from dcpy.library.archive import Archive

from . import (
    TEST_DATASET_CONFIG_FILE,
    TEST_DATASET_NAME,
    TEST_DATASET_OUTPUT_DIRECTORY,
    TEST_DATASET_OUTPUT_PATH,
    TEST_DATASET_OUTPUT_PATH_S3,
    TEST_DATASET_VERSION,
    pg,
    recipe_engine,
)

archive = Archive()


def start_clean(local_files: list, s3_files: list):
    for f in local_files:
        if os.path.isfile(f):
            os.remove(f)
    for f in s3_files:
        if archive.s3.exists(f):
            archive.s3.rm(f)


def test_archive_1():
    local_not_exist = [
        f"{TEST_DATASET_OUTPUT_PATH}.zip",
        f"{TEST_DATASET_OUTPUT_PATH}.csv",
    ]
    s3_exist = [
        f"{TEST_DATASET_OUTPUT_PATH_S3}/{TEST_DATASET_NAME}.csv.zip",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.csv.zip",
        f"{TEST_DATASET_OUTPUT_PATH_S3}/config.json",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.csv.zip",
        f"datasets/{TEST_DATASET_NAME}/latest/config.json",
    ]
    start_clean(local_not_exist, s3_exist)
    archive(
        f"{TEST_DATASET_CONFIG_FILE}",
        output_format="csv",
        push=True,
        clean=True,
        latest=True,
        compress=True,
    )
    for f in local_not_exist:
        assert not os.path.isfile(f)
    for f in s3_exist:
        assert archive.s3.exists(f)
    start_clean(local_not_exist, s3_exist)


def test_archive_2():
    s3_not_exist = [
        f"{TEST_DATASET_OUTPUT_PATH_S3}/{TEST_DATASET_NAME}.geojson.zip",
        f"{TEST_DATASET_OUTPUT_PATH_S3}/config.json",
        f"{TEST_DATASET_OUTPUT_PATH_S3}/{TEST_DATASET_NAME}.geojson",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.geojson.zip",
        f"datasets/{TEST_DATASET_NAME}/latest/config.json",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.geojson",
    ]
    local_exist = [
        f"{TEST_DATASET_OUTPUT_DIRECTORY}/config.json",
        f"{TEST_DATASET_OUTPUT_PATH}.geojson.zip",
        f"{TEST_DATASET_OUTPUT_PATH}.geojson",
    ]
    start_clean(local_exist, s3_not_exist)
    archive(
        f"{TEST_DATASET_CONFIG_FILE}",
        output_format="geojson",
        push=False,
        clean=False,
        latest=True,
        compress=True,
    )
    for f in s3_not_exist:
        assert not archive.s3.exists(f)
    for f in local_exist:
        assert os.path.isfile(f)
    start_clean(local_exist, s3_not_exist)


def test_archive_3():
    archive(
        f"{TEST_DATASET_CONFIG_FILE}",
        output_format="postgres",
        postgres_url=recipe_engine,
    )
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


def test_archive_4():
    s3_not_exist = [
        f"datasets/{TEST_DATASET_NAME}/testor/{TEST_DATASET_NAME}.csv.zip",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.csv.zip",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.csv",
        f"datasets/{TEST_DATASET_NAME}/latest/config.json",
    ]
    s3_exist = [
        f"datasets/{TEST_DATASET_NAME}/testor/config.json",
        f"datasets/{TEST_DATASET_NAME}/testor/{TEST_DATASET_NAME}.csv",
    ]
    local_exist = [
        f".library/datasets/{TEST_DATASET_NAME}/testor/{TEST_DATASET_NAME}.csv",
        f".library/datasets/{TEST_DATASET_NAME}/testor/config.json",
    ]
    local_not_exist = [
        f".library/datasets/{TEST_DATASET_NAME}/testor/{TEST_DATASET_NAME}.csv.zip"
    ]
    start_clean(local_exist + local_not_exist, s3_exist + s3_not_exist)
    archive(
        f"{TEST_DATASET_CONFIG_FILE}",
        output_format="csv",
        push=True,
        clean=False,
        compress=False,
        latest=False,
        version="testor",
    )
    for f in local_exist:
        assert os.path.isfile(f)
    for f in local_not_exist:
        assert not os.path.isfile(f)
    for f in s3_exist:
        assert archive.s3.exists(f)
    for f in s3_not_exist:
        assert not archive.s3.exists(f)
    start_clean(local_exist + local_not_exist, s3_exist + s3_not_exist)


def test_archive_5():
    s3_not_exist = [
        f"datasets/{TEST_DATASET_NAME}/testor/{TEST_DATASET_NAME}.csv.zip",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.csv.zip",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.csv",
        f"datasets/{TEST_DATASET_NAME}/latest/config.json",
    ]
    s3_exist = [
        f"datasets/{TEST_DATASET_NAME}/testor/config.json",
        f"datasets/{TEST_DATASET_NAME}/testor/{TEST_DATASET_NAME}.csv",
    ]
    local_exist = [
        f".library/datasets/{TEST_DATASET_NAME}/testor/{TEST_DATASET_NAME}.csv",
        f".library/datasets/{TEST_DATASET_NAME}/testor/config.json",
    ]
    local_not_exist = [
        f".library/datasets/{TEST_DATASET_NAME}/testor/{TEST_DATASET_NAME}.csv.zip"
    ]
    start_clean(local_exist + local_not_exist, s3_exist + s3_not_exist)
    archive(
        name=f"{TEST_DATASET_NAME}",
        output_format="csv",
        push=True,
        clean=False,
        compress=False,
        latest=False,
        version="testor",
    )
    for f in local_exist:
        assert os.path.isfile(f)
    for f in local_not_exist:
        assert not os.path.isfile(f)
    for f in s3_exist:
        assert archive.s3.exists(f)
    for f in s3_not_exist:
        assert not archive.s3.exists(f)
    start_clean(local_exist + local_not_exist, s3_exist + s3_not_exist)


def test_archive_6():
    s3_not_exist = [
        f"{TEST_DATASET_OUTPUT_PATH_S3}/{TEST_DATASET_NAME}.shp",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.shp",
    ]
    local_not_exist = [f"{TEST_DATASET_OUTPUT_PATH}.shp"]
    s3_exist = [
        f"{TEST_DATASET_OUTPUT_PATH_S3}/{TEST_DATASET_NAME}.shp.zip",
        f"datasets/{TEST_DATASET_NAME}/latest/{TEST_DATASET_NAME}.shp.zip",
        f"{TEST_DATASET_OUTPUT_PATH_S3}/config.json",
        f"datasets/{TEST_DATASET_NAME}/latest/config.json",
    ]
    local_exist = [f"{TEST_DATASET_OUTPUT_PATH}.shp.zip"]
    start_clean(local_exist + local_not_exist, s3_exist + s3_not_exist)
    archive(
        f"{TEST_DATASET_CONFIG_FILE}",
        output_format="shapefile",
        push=True,
        clean=False,
        latest=True,
    )
    for f in local_exist:
        assert os.path.isfile(f)
    for f in local_not_exist:
        assert not os.path.isfile(f)
    for f in s3_exist:
        assert archive.s3.exists(f)
    for f in s3_not_exist:
        assert not archive.s3.exists(f)
    start_clean(local_exist + local_not_exist, s3_exist + s3_not_exist)
