"""District Review: our district layers vs the previous release's, per district.

Reads the summary and summary_by_layer tables of validation_output/district_review.gdb.zip,
written in the QA stage by products/cscl/poc_validation/district_review_gdb.py. The tables
are read in place with GDAL range requests, so the (~150 MB) zip isn't downloaded.
"""

import pandas as pd
import pyogrio
import streamlit as st

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.connectors.edm.models import BuildKey
from dcpy.utils import s3

from .helpers import PRODUCT

DISTRICT_REVIEW_FILE = "validation_output/district_review.gdb.zip"
GDB_NAME = "district_review.gdb"
CLASSES = ["major", "minor", "new_only", "prior_only", "grid_noise", "identical"]
DEFAULT_CLASSES = ["major", "minor", "new_only", "prior_only"]


def _key(build: str) -> str:
    return f"{BuildKey(PRODUCT, build).path}/{DISTRICT_REVIEW_FILE}"


def get_download_url(build: str) -> str:
    return s3.get_presigned_get_url(PUBLISHING_BUCKET, _key(build))


@st.cache_data(show_spinner=False, ttl=600)
def get_district_review(build: str) -> dict[str, pd.DataFrame] | None:
    """The FGDB's summary tables, or None for builds without district_review.gdb.zip."""
    if not s3.object_exists(PUBLISHING_BUCKET, _key(build)):
        return None
    # presigned GET urls refuse HEAD requests
    pyogrio.set_gdal_config_options({"CPL_VSIL_CURL_USE_HEAD": "NO"})
    path = f"/vsizip/{{/vsicurl/{get_download_url(build)}}}/{GDB_NAME}"
    return {
        layer: pyogrio.read_dataframe(path, layer=layer, read_geometry=False)
        for layer in ["summary", "summary_by_layer"]
    }


def render(build: str) -> None:
    st.subheader("District Review")
    st.markdown(
        """
        Each district layer compared with the previous release's, district by district
        (`district_review.gdb.zip`). **class**: identical, grid_noise (differences below
        the coordinate grid), minor, major, or new_only / prior_only (district on one side
        only). The FGDB holds, per layer, `<layer>_new`, `<layer>_prior` and `<layer>_diff`
        (symmetric-difference pieces) for review in a GIS.
        """
    )
    with st.spinner("Loading district review tables..."):
        tables = get_district_review(build)
    if tables is None:
        st.info(
            f"No district review for this build: `{DISTRICT_REVIEW_FILE}` is written by "
            "the QA stage's district_review_gdb step, which this build predates."
        )
        return

    st.markdown(f"[Download district_review.gdb.zip]({get_download_url(build)})")

    by_layer = tables["summary_by_layer"]
    st.markdown("**Per layer**")
    st.dataframe(
        by_layer.sort_values(["n_major", "n_minor"], ascending=False),
        width="stretch",
        hide_index=True,
    )

    summary = tables["summary"]
    st.markdown(
        "**Per district** (every non-identical district, ranked by symdiff_area_pct)"
    )
    col1, col2, col3 = st.columns(3)
    layers = col1.multiselect(
        "Layers",
        options=sorted(summary["layer"].unique()),
        placeholder="All layers",
        key="district_review_layers",
    )
    present = [c for c in CLASSES if c in set(summary["class"])]
    classes = col2.multiselect(
        "Class",
        options=present,
        default=[c for c in DEFAULT_CLASSES if c in present],
        key="district_review_classes",
    )
    district = col3.text_input("District key contains", key="district_review_key")

    filtered = summary
    if layers:
        filtered = filtered[filtered["layer"].isin(layers)]
    if classes:
        filtered = filtered[filtered["class"].isin(classes)]
    if district:
        filtered = filtered[
            filtered["district_key"].astype(str).str.contains(district, regex=False)
        ]
    st.caption(f"{len(filtered):,} of {len(summary):,} districts")
    st.dataframe(
        filtered.head(5000),
        width="stretch",
        hide_index=True,
        column_config={
            "symdiff_area_pct": st.column_config.NumberColumn(format="%.4f"),
            "area_delta_pct": st.column_config.NumberColumn(format="%.4f"),
        },
    )
    if len(filtered) > 5000:
        st.caption("Showing the first 5,000; filter to narrow it down.")
