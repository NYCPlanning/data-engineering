"""Field-Level Diffs: per output, which fields differ from prod, and whether each
difference is accounted for (a known difference) or not.

Reads validation_output/field_diffs.parquet, written in the QA stage by
products/cscl/poc_validation/export_field_diffs.py from the qa__diffs_* models.
"""

import pandas as pd
import streamlit as st

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.connectors.edm.models import BuildKey
from dcpy.lifecycle.builds import builds
from dcpy.utils import s3

from .helpers import PRODUCT

FIELD_DIFFS_FILE = "validation_output/field_diffs.parquet"
# rows that exist on one side only have no changed fields
WHOLE_ROW = "(whole row)"


@st.cache_data(show_spinner=False, ttl=600)
def get_field_diffs(build: str) -> pd.DataFrame | None:
    """The build's field diffs, or None for builds made before the QA stage wrote them."""
    key = f"{BuildKey(PRODUCT, build).path}/{FIELD_DIFFS_FILE}"
    if not s3.object_exists(PUBLISHING_BUCKET, key):
        return None
    df = builds.read_parquet(PRODUCT, build, FIELD_DIFFS_FILE)
    df["field"] = df["field"].fillna(WHOLE_ROW)
    return df


def summarize_outputs(df: pd.DataFrame) -> pd.DataFrame:
    """One row per output: discrepant rows (accounted/unaccounted) and changed fields."""
    rows = df.drop_duplicates(["output_file_id", "comparison_id"])
    by_output = rows.groupby("output_file_id").agg(
        discrepant_rows=("comparison_id", "size"),
        accounted_rows=("accounted_for", "sum"),
    )
    by_output["unaccounted_rows"] = (
        by_output["discrepant_rows"] - by_output["accounted_rows"]
    )
    changed = df[df["field"] != WHOLE_ROW]
    by_output["fields_changed"] = changed.groupby("output_file_id")["field"].nunique()
    by_output["unaccounted_field_changes"] = (
        changed[~changed["accounted_for"]].groupby("output_file_id").size()
    )
    by_output = by_output.fillna(0).astype(int).reset_index()
    return by_output.sort_values(
        ["unaccounted_rows", "discrepant_rows"], ascending=False
    )


def summarize_fields(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (field, status, accounted_for, known difference, diff_group)."""
    keys = [
        "field",
        "status",
        "accounted_for",
        "known_difference_id",
        "diff_group",
    ]
    out = (
        df.groupby(keys, dropna=False)
        .agg(
            rows=("comparison_id", "nunique"),
            known_difference_description=("known_difference_description", "first"),
        )
        .reset_index()
    )
    return out.sort_values(["accounted_for", "rows"], ascending=[True, False])


def render(build: str) -> None:
    st.subheader("Field-Level Diffs")
    st.markdown(
        """
        Keyed, field-by-field comparison of each output against prod, from the `qa__diffs_*`
        models. A difference is **accounted for** when it's a known or expected one; where it
        cites an entry in `seeds/known_differences.csv`, its ID and description are shown.
        """
    )
    with st.spinner("Loading field-level diffs..."):
        df = get_field_diffs(build)
    if df is None:
        st.info(
            f"No field-level diffs for this build: `{FIELD_DIFFS_FILE}` is written by the "
            "QA stage's export_field_diffs step, which this build predates."
        )
        return
    if df.empty:
        st.success("No field-level diffs in this build.")
        return

    st.dataframe(summarize_outputs(df), width="stretch", hide_index=True)

    outputs = sorted(df["output_file_id"].unique())
    output = st.selectbox("Select an output", options=outputs, key="field_diffs_output")
    if not output:
        return
    output_df = df[df["output_file_id"] == output]

    show = st.radio(
        "Show",
        options=["All", "Unaccounted", "Accounted for"],
        horizontal=True,
        key="field_diffs_show",
    )
    if show == "Unaccounted":
        output_df = output_df[~output_df["accounted_for"]]
    elif show == "Accounted for":
        output_df = output_df[output_df["accounted_for"]]
    if output_df.empty:
        st.info("Nothing to show.")
        return

    st.dataframe(
        summarize_fields(output_df),
        width="stretch",
        hide_index=True,
        column_config={
            "accounted_for": st.column_config.CheckboxColumn("accounted_for"),
            "known_difference_description": st.column_config.TextColumn(
                "known_difference_description", width="large"
            ),
        },
    )

    fields = output_df["field"].value_counts().index.tolist()
    field = st.selectbox(
        "Select a field to see example rows", options=fields, key="field_diffs_field"
    )
    if not field:
        return
    examples = output_df[output_df["field"] == field]
    limit = st.number_input(
        "Example rows", min_value=10, max_value=5000, value=50, step=50
    )
    key_column = examples["comparison_column"].iloc[0]
    st.caption(
        f"{len(examples)} rows; keyed on `{key_column}`. "
        "prod_value / dev_value are empty for rows on one side only."
    )
    st.dataframe(
        examples[
            [
                "comparison_id",
                "status",
                "prod_value",
                "dev_value",
                "accounted_for",
                "known_difference_id",
                "diff_group",
            ]
        ].head(int(limit)),
        width="stretch",
        hide_index=True,
    )
