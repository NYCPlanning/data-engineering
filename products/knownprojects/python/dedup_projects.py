from dcpy.utils import postgres
from dcpy.utils.logging import logger
from psycopg2.extensions import AsIs

from . import BUILD_ENGINE_SCHEMA

INPUT_TABLE = "_kpdb_projects"
OUTPUT_TABLE = "_kpdb"

PRIMARY_KEY = "project_id"

if __name__ == "__main__":
    logger.info(f"Starting KPDB project deduplication")
    pg_client = postgres.PostgresClient(schema=BUILD_ENGINE_SCHEMA)

    logger.info(f"Getting data from table '{INPUT_TABLE}' ...")
    kpdb = pg_client.get_table(INPUT_TABLE, geometry_col="geom")
    logger.info(f"Shape of data:\n\t{kpdb.shape}")
    logger.info(f"Columns:\n\t{kpdb.columns.to_list()}")

    logger.info(
        f"Excluding the primary key '{PRIMARY_KEY}' from the columns used to identify duplicates"
    )
    columns_to_consider = [col for col in kpdb.columns.to_list() if col != PRIMARY_KEY]

    # using DataFrame.astype(str) to avoid an error caused by apparent list data
    kpdb_no_duplicates = kpdb.loc[
        kpdb.astype(str).drop_duplicates(subset=columns_to_consider).index
    ]
    logger.info(f"Shape of deduplicated data:\n\t{kpdb_no_duplicates.shape}")

    records_dropped_count = kpdb.shape[0] - kpdb_no_duplicates.shape[0]
    logger.info(f"# of duplicate records removed:\n\t{records_dropped_count}")

    logger.info(f"Creating table '{OUTPUT_TABLE}' ...")
    pg_client.insert_dataframe(df=kpdb_no_duplicates, table_name=OUTPUT_TABLE)
    pg_client.execute_query(
        """
        ALTER TABLE :table_name
        ALTER COLUMN :geometry_col type Geometry
        USING ST_SetSRID(:geometry_col, 4326);
        """,
        placeholders={
            "table_name": AsIs(OUTPUT_TABLE),
            "geometry_col": AsIs("geom"),
        },
    )
