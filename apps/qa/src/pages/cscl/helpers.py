import pandas as pd
import streamlit as st

from dcpy.connectors.edm import publishing
from dcpy.connectors.edm.models import BuildKey
from dcpy.utils import postgres, s3

PRODUCT = "db-cscl"
PROD_BUCKET = "edm-private"

# dbt QA models in etl_dev_qa/ and the files they relate to (for display grouping)
# Schema = build name (BUILD_ENGINE_SCHEMA is set to the build name in CI)
DBT_QA_TABLES: dict[str, list[str]] = {
    "All": [
        "qa__diffs_all",
    ],
    "LION DAT": [
        "qa__lion_dat_summary",
        "qa__lion_dat_by_row",
        "qa__lion_dat_individual_diffs",
    ],
    "ThinLION": [
        "qa__thinlion_all_comparison",
        "qa__thinlion_bronx_comparison",
        "qa__thinlion_brooklyn_comparison",
        "qa__thinlion_manhattan_comparison",
        "qa__thinlion_queens_comparison",
        "qa__thinlion_statenisland_comparison",
    ],
    "SAF": [
        "qa__diffs_saf_abcegnpx_roadbed",
    ],
    "RPL": [
        "qa__rpl_order",
        "qa__rpl_order_diffs",
    ],
}


def get_builds() -> list[str]:
    return publishing.get_builds(PRODUCT)


def get_build_version(build: str) -> str:
    product_key = BuildKey(PRODUCT, build)
    return publishing.get_build_metadata(product_key).version


@st.cache_data(show_spinner=False, ttl=60)
def get_diffs_summary(build: str) -> pd.DataFrame:
    product_key = BuildKey(PRODUCT, build)
    return publishing.read_csv(product_key, "validation_output/diffs_summary.csv")


@st.cache_data(show_spinner=False, ttl=60)
def get_diff_rows(build: str, filename: str) -> list[str]:
    """Stream dev and prod files from S3 and return lines present in dev but not prod.

    Equivalent to comm -23 <(sort dev) <(sort prod).
    Cached so repeated selections don't re-fetch from S3.
    """
    build_key = BuildKey(PRODUCT, build)
    prod_version = get_build_version(build)

    dev_buffer = publishing.get_file(build_key, filename)
    dev_lines = {
        line
        for line in dev_buffer.read().decode("latin-1").splitlines()
        if line.strip()
    }

    prod_buffer = s3.get_file_as_stream(
        PROD_BUCKET, f"cscl_etl/{prod_version}/{filename}"
    )
    prod_lines = {
        line
        for line in prod_buffer.read().decode("latin-1").splitlines()
        if line.strip()
    }

    return sorted(dev_lines - prod_lines)


def get_build_output_zip_url(build: str) -> str:
    """Return a direct public download URL for the build's output.zip."""
    from dcpy.utils import s3

    build_key = BuildKey(PRODUCT, build)
    bucket = publishing.PUBLISHING_BUCKET
    key = f"{build_key.path}/output.zip"
    return s3.get_presigned_get_url(bucket, key)


def get_pg_client(build: str) -> postgres.PostgresClient:
    """Return a PostgresClient scoped to the dbt schema for the given build."""
    return postgres.PostgresClient(
        database=PRODUCT, schema=build.lower().replace("-", "_")
    )


def get_build_tables(build: str) -> list[str]:
    """List all tables in the build's Postgres schema."""
    client = get_pg_client(build)
    return client.get_schema_tables()


@st.cache_data(show_spinner=False, ttl=60)
def get_dbt_qa_table(build: str, table_name: str) -> pd.DataFrame:
    """Read a dbt QA model table from Postgres into a DataFrame."""
    client = get_pg_client(build)
    return client.read_table_df(table_name, schema=client.schema)


def get_record_by_comparison_id(
    build: str, table_name: str, id_column: str, id_value: str
) -> pd.DataFrame:
    """Fetch a single record from a build table by its primary key value."""
    client = get_pg_client(build)
    return client.execute_select_query(
        f'SELECT * FROM "{table_name}" WHERE "{id_column}" = :id_value',
        id_value=id_value,
    )


@st.cache_data(show_spinner=False, ttl=60)
def get_build_table(build: str, table_name: str) -> pd.DataFrame:
    """Read any table from the build's Postgres schema into a DataFrame."""
    client = get_pg_client(build)
    return client.read_table_df(table_name, schema=client.schema)
