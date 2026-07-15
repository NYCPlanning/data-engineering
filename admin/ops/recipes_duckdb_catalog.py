"""
This script, locally and in edm-recipes/datasets/catalog.duckdb, creates a snapshot
catalog of all parquet datasets in edm-recipes/datasets. It does this by creating
one view per file: There is one schema created per dataset and one table per version,
such that they can be queried as `{dataset}.{version}`
"""

import time
from pathlib import Path

import duckdb  # type: ignore

from dcpy.connectors.edm import recipes
from dcpy.utils import duckdb as dcpy_duckdb
from dcpy.utils import s3
from dcpy.utils.logging import logger

PROGRESS_INTERVAL = 25

bucket = recipes._bucket()
local_file = Path("./catalog.duckdb")


def version_file_types(dataset: str) -> dict[str, set[recipes.DatasetType]]:
    """File types present for every version of a dataset, keyed by version.

    One S3 listing per dataset instead of one per version - avoids the O(datasets x
    versions) round-trips that made this script take 30-60 minutes.
    """
    prefix = f"{recipes.DATASET_FOLDER}/{dataset}/"
    types_by_version: dict[str, set[recipes.DatasetType]] = {}
    for obj in s3.list_objects(bucket, prefix):
        version, _, filename = obj["Key"].removeprefix(prefix).partition("/")
        if not filename:
            continue
        file_type = recipes.DatasetType.from_extension(Path(filename).suffix.strip("."))
        if file_type is not None:
            types_by_version.setdefault(version, set()).add(file_type)
    return types_by_version


local_file.unlink(missing_ok=True)
conn = duckdb.connect(local_file)
dcpy_duckdb.setup_s3_secret(conn)
conn.sql("install spatial; load spatial;")

run_start = time.monotonic()

logger.info(f"listing datasets in {bucket}/datasets/...")
datasets = s3.get_subfolders(bucket, "datasets/")
listing_elapsed = time.monotonic() - run_start
logger.info(f"found {len(datasets)} datasets in {listing_elapsed:.0f}s")

datasets_with_parquet = 0
views_created = 0
processing_start = time.monotonic()

for i, dataset in enumerate(datasets, start=1):
    logger.debug(dataset)
    types_by_version = version_file_types(dataset)
    if recipes.DatasetType.parquet in types_by_version.get("latest", set()):
        datasets_with_parquet += 1
        conn.sql(f"CREATE OR REPLACE SCHEMA {dataset}")
        for version, file_types in types_by_version.items():
            if version == "latest":
                continue
            if recipes.DatasetType.parquet in file_types:
                views_created += 1
                conn.sql(
                    f"""
                        CREATE VIEW {dataset}."{version}" AS
                        SELECT *
                        FROM 's3://edm-recipes/datasets/{dataset}/{version}/{dataset}.parquet';
                    """
                )
    if i % PROGRESS_INTERVAL == 0 or i == len(datasets):
        elapsed = time.monotonic() - processing_start
        logger.info(f"processed {i}/{len(datasets)} datasets ({elapsed:.0f}s)")

processing_elapsed = time.monotonic() - processing_start

conn.commit()
conn.close()

upload_start = time.monotonic()
s3.upload_file(bucket, local_file, "datasets/catalog.duckdb", "private")
upload_elapsed = time.monotonic() - upload_start

total_elapsed = time.monotonic() - run_start
logger.info(
    f"done in {total_elapsed:.0f}s "
    f"(listing={listing_elapsed:.0f}s, processing={processing_elapsed:.0f}s, "
    f"upload={upload_elapsed:.0f}s) - {datasets_with_parquet}/{len(datasets)} "
    f"datasets have parquet, {views_created} views created"
)
