from dcpy.utils import postgres
from dcpy.utils.logging import logger

from . import BUILD_ENGINE_SCHEMA

FINAL_TABLE_NAME = "_kpdb"
INTERMEDIATE_TABLE_NAME = "_kpdb_before_deduplication"

PRIMARY_KEY = "project_id"

if __name__ == "__main__":
    logger.info(f"Starting KPDB project deduplication")
    pg_client = postgres.PostgresClient(schema=BUILD_ENGINE_SCHEMA)

    logger.info(f"Getting data from table '{FINAL_TABLE_NAME}' ...")
    kpdb = pg_client.get_table(FINAL_TABLE_NAME)
    logger.info(f"Shape of data:\n\t{kpdb.shape}")
    logger.info(f"Columns:\n\t{kpdb.columns.to_list()}")

    logger.info(
        f"Excluding the primary key '{PRIMARY_KEY}' from the columns used to identify duplicates"
    )
    columns_to_consider = [col for col in kpdb.columns.to_list() if col != PRIMARY_KEY]

    # use DataFrame.astype(str) to avoid error from data of type list
    kpdb_no_duplicates = kpdb.loc[
        kpdb.astype(str).drop_duplicates(subset=columns_to_consider).index
    ]
    logger.info(f"Shape of deduplicated data:\n\t{kpdb_no_duplicates.shape}")

    records_dropped_count = kpdb.shape[0] - kpdb_no_duplicates.shape[0]
    logger.info(f"# of duplicate records removed:\n\t{records_dropped_count}")

    # logger.info(f"Saving original data as '{INTERMEDIATE_TABLE_NAME}' ...")
    # pg_client.insert_dataframe(df=kpdb, table_name=INTERMEDIATE_TABLE_NAME)

    # logger.info(f"Updating final table '{FINAL_TABLE_NAME}' ...")
    # pg_client.insert_dataframe(df=kpdb_no_duplicates, table_name=FINAL_TABLE_NAME)
