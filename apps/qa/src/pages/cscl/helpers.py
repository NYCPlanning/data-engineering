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
    "RPL": [
        "qa__rpl_order",
        "qa__rpl_order_diffs",
    ],
}


@st.cache_data(show_spinner=False)
def get_builds() -> list[str]:
    return publishing.get_builds(PRODUCT)


@st.cache_data(show_spinner=False)
def get_build_version(build: str) -> str:
    product_key = BuildKey(PRODUCT, build)
    return publishing.get_build_metadata(product_key).version


@st.cache_data(show_spinner=False)
def get_diffs_summary(build: str) -> pd.DataFrame:
    product_key = BuildKey(PRODUCT, build)
    return publishing.read_csv(product_key, "validation_output/diffs_summary.csv")


@st.cache_data(show_spinner=False)
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


def get_pg_client(build: str) -> postgres.PostgresClient:
    """Return a PostgresClient scoped to the dbt schema for the given build.

    In CI, BUILD_ENGINE_SCHEMA is set to the build name, so the dbt tables
    for build 'nightly_qa' live in schema 'nightly_qa'.
    """
    return postgres.PostgresClient(database=PRODUCT, schema=build)


@st.cache_data(show_spinner=False)
def get_dbt_qa_table(build: str, table_name: str) -> pd.DataFrame:
    """Read a dbt QA model table from Postgres into a DataFrame."""
    client = get_pg_client(build)
    return client.read_table_df(table_name)
