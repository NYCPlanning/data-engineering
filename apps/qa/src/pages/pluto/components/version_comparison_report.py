import streamlit as st
from src.shared.components import build_outputs

from .aggregate_report import AggregateReport
from .bbl_diffs_report import BblDiffsReport
from .expected_value_differences_report import (
    ExpectedValueDifferencesReport,
)
from .mismatch_report import MismatchReport
from .null_graph import NullReport
from .outlier_report import OutlierReport
from .source_data_versions_report import (
    SourceDataVersionsReport,
)


def version_comparison_report(product_key, data, comp_type):
    versions = sorted(data["df_aggregate"]["v"].unique(), reverse=True)

    v = versions[0]
    v_prev = versions[versions.index(v) + 1]

    if comp_type == "Previous":
        v_comp = v_prev
    else:
        v_comp = next(_v for _v in versions if _v != v and (("." in _v) == ("." in v)))
    v_comp_prev = versions[versions.index(v_comp) + 1]

    condo = st.sidebar.checkbox("condo only")
    mapped = st.sidebar.checkbox("mapped only")

    build_outputs.data_directory_link(product_key)

    st.markdown(
        f"""
        **{v}** is the Current version

        **{v_prev}** is the Previous version

        **{v_comp} - {v_comp_prev}** is the diff for comparison
    """
    )
    st.markdown(
        f"""
        This series of reports compares two pairs of PLUTO versions using two colors in graphs:
        - blue: the Selected and the Previous versions ({v})
        - gold: the previous two versions ({v_prev})

        The graphs report the number of records that have a different value in a given field but share the same BBL between versions.

        In this series of graphs the x-axis is the field name and the y-axis is the total number of records. \

        The graphs are useful to see if there are any dramatic changes in the values of fields between versions.

        For example, you can read these graphs as "there are 300,000 records with the same BBL between {v} to {v_prev}, but the exempttot value changed."

        Hover over the graph to see the percent of records that have a change.

        There is an option to filter these graphs to just show condo lots. \
        Condos make up a small percentage of all lots, but they contain a large percentage of the residential housing. \
        A second filter enables you look at all lots or just mapped lots. \
        Unmapped lots are those with a record in PTS, but no corresponding record in DTM. \
        This happens because DOF updates are not in sync.

    """
    )

    SourceDataVersionsReport(version_text=data["version_text"])()

    MismatchReport(
        data=data["df_mismatch"],
        v=v,
        v_prev=v_prev,
        v_comp=v_comp,
        v_comp_prev=v_comp_prev,
        condo=condo,
        mapped=mapped,
    )()

    AggregateReport(
        data=data["df_aggregate"],
        v=v,
        v_prev=v_prev,
        v_comp=v_comp,
        v_comp_prev=v_comp_prev,
        condo=condo,
        mapped=mapped,
    )()

    NullReport(
        data=data["df_null"],
        v=v,
        v_prev=v_prev,
        v_comp=v_comp,
        v_comp_prev=v_comp_prev,
        condo=condo,
        mapped=mapped,
    )()

    ExpectedValueDifferencesReport(data=data["df_expected"], v=v, v_prev=v_prev)()

    OutlierReport(
        data=data["df_outlier"], v=v, v_prev=v_prev, condo=condo, mapped=mapped
    )()

    BblDiffsReport(data=data.get("df_bbl_diffs", None))()
