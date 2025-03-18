from pathlib import Path
import duckdb

from dcpy.utils import s3, duckdb as dcpy_duckdb
from dcpy.connectors.edm import recipes

local_file = Path("./catalog.duckdb")

local_file.unlink(missing_ok=True)
conn = duckdb.connect(local_file)
dcpy_duckdb.setup_s3_secret(conn)
conn.sql("install spatial; load spatial;")

for i, dataset in enumerate(s3.get_subfolders(recipes.BUCKET, "datasets/")):
    print(dataset)
    if recipes.DatasetType.parquet in recipes.get_file_types(
        recipes.DatasetKey(id=dataset, version="latest")
    ):
        conn.sql(f"CREATE OR REPLACE SCHEMA {dataset}")
        for version in recipes.get_all_versions(dataset):
            if recipes.DatasetType.parquet in recipes.get_file_types(
                recipes.DatasetKey(id=dataset, version=version)
            ):
                conn.sql(
                    f"""
                        CREATE VIEW {dataset}."{version}" AS 
                        SELECT * 
                        FROM 's3://edm-recipes/datasets/{dataset}/{version}/{dataset}.parquet';
                    """
                )

conn.commit()
conn.close()

s3.upload_file(recipes.BUCKET, local_file, "datasets/catalog.duckdb", "private")
