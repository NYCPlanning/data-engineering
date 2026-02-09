import pandas as pd
import streamlit as st
from src import QAQC_DB, QAQC_DB_SCHEMA_SOURCE_DATA
from src.shared.utils.source_report import (
    compare_source_data_columns,
    compare_source_data_row_count,
    dataframe_style_source_report_results,
    get_source_data_versions_to_compare,
    load_source_data_to_compare,
)

from dcpy.connectors.edm import publishing
from dcpy.utils import postgres


def sources_report(
    reference_product_key: publishing.ProductKey,
    staging_product_key: publishing.ProductKey,
):
    print("STARTING Source Data Review")
    st.header("Source Data Review")
    st.markdown(
        f"""
    This page reviews the status of all source data used to build this dataset.
    It compares the latest versions of source data to those used in the build of a reference version of this dataset.
    
    The reference dataset version is `{reference_product_key}`.
    """
    )
    pg_client = postgres.PostgresClient(
        database=QAQC_DB,
        schema=QAQC_DB_SCHEMA_SOURCE_DATA,
    )

    st.subheader("Compare source data versions")
    source_data_versions = get_source_data_versions_to_compare(
        reference_product_key, staging_product_key
    )

    st.dataframe(source_data_versions)
    source_report_results = source_data_versions.to_dict(orient="index")

    source_dataset_names = source_report_results.keys()
    # source_dataset_names = get_source_dataset_names(dataset, reference_version)
    # source_dataset_names = ["dcp_zoningmapamendments", "dcp_limitedheight"]
    # # remove non-dev source datasets from full source_report_results
    # source_report_results = {
    #     dataset_name: source_report_results[dataset_name]
    #     for dataset_name in source_dataset_names
    # }
    # st.warning(f"Only using DEV source datasets `{source_dataset_names}`")

    if not st.session_state.get("source_load_button", False):
        st.session_state.data_loaded = False
        st.button(
            label="⬇️ Load source data",
            use_container_width=True,
            key="source_load_button",
        )
        return

    st.button(
        label="🔄 Refresh page to reload source data",
        use_container_width=True,
        key="source_load_button",
        disabled=True,
    )

    data_loading_expander = st.expander("Data loading status")
    with data_loading_expander:
        for dataset in source_dataset_names:
            with st.spinner(f"⏳ Loading {dataset} versions ..."):
                status_messages = load_source_data_to_compare(
                    dataset=dataset,
                    source_data_versions=source_data_versions,
                    pg_client=pg_client,
                )
            success_message = "\n\n".join(status_messages)
            st.success(success_message)

    # TODO (nice-to-have) consider adding table names to source_report_results
    st.subheader("Compare source data shapes")
    with st.spinner("⏳ Comparing columns ..."):
        source_report_results = compare_source_data_columns(
            source_report_results, pg_client=pg_client
        )
    with st.spinner("⏳ Comparing row counts ..."):
        source_report_results = compare_source_data_row_count(
            source_report_results, pg_client=pg_client
        )

    df_source_report_results = pd.DataFrame.from_dict(
        source_report_results, orient="index"
    )
    st.dataframe(
        df_source_report_results.style.applymap(  # type: ignore
            dataframe_style_source_report_results,
            subset=["same_columns", "same_row_count"],
        )
    )
    with st.expander("DEV DEBUG SECTION"):
        st.dataframe(df_source_report_results)
        st.table(df_source_report_results)
        st.json(source_report_results)
