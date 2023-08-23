def pluto():
    import streamlit as st

    from src.pluto.helpers import get_s3_folders, get_data, get_past_versions
    from src.pluto.components.changes_report import ChangesReport
    from src.pluto.components.mismatch_report import MismatchReport
    from src.pluto.components.null_graph import NullReport
    from src.pluto.components.source_data_versions_report import (
        SourceDataVersionsReport,
    )
    from src.pluto.components.expected_value_differences_report import (
        ExpectedValueDifferencesReport,
    )
    from src.pluto.components.outlier_report import OutlierReport
    from src.pluto.components.aggregate_report import AggregateReport

    st.title("PLUTO QAQC")

    branches = get_s3_folders()
    branch = st.sidebar.selectbox(
        "Choose a build name (S3 folder)",
        branches,
        index=branches.index("main"),
    )

    report_type = st.sidebar.selectbox(
        "Choose a Report Type",
        ["Compare with Previous Version", "Review Manual Changes"],
    )

    data = get_data(branch)

    def version_comparison_report(data):
        versions = get_past_versions()

        v1 = st.sidebar.selectbox(
            "Choose a version of PLUTO",
            versions,
        )
        v2 = st.sidebar.selectbox(
            "Choose a Previous version of PLUTO",
            versions,
            versions.index(v1) + 1,
        )
        v3 = st.sidebar.selectbox(
            "Choose a Previous Previous of PLUTO",
            versions,
            versions.index(v1) + 2,
        )

        condo = st.sidebar.checkbox("condo only")
        mapped = st.sidebar.checkbox("mapped only")

        st.markdown(
            f"""
            **{v1}** is the Current version

            **{v2}** is the Previous version

            **{v3}** is the Previous Previous version
        """
        )
        st.markdown(
            f"""
            This series of reports compares two pairs of PLUTO versions using two colors in graphs:
            - blue: the Celected and the Previous versions ({v1} and {v2})
            - gold: the previous two versions ({v2} and {v3})

            The graphs report the number of records that have a different value in a given field but share the same BBL between versions.

            In this series of graphs the x-axis is the field name and the y-axis is the total number of records. \

            The graphs are useful to see if there are any dramatic changes in the values of fields between versions.

            For example, you can read these graphs as "there are 300,000 records with the same BBL between {v1} to {v2}, but the exempttot value changed."

            Hover over the graph to see the percent of records that have a change.

            There is an option to filter these graphs to just show condo lots. \
            Condos make up a small percentage of all lots, but they contain a large percentage of the residential housing. \
            A second filter enables you look at all lots or just mapped lots. \
            Unmapped lots are those with a record in PTS, but no corresponding record in DTM. \
            This happens because DOF updates are not in sync.

        """
        )

        MismatchReport(
            data=data["df_mismatch"], v1=v1, v2=v2, v3=v3, condo=condo, mapped=mapped
        )()

        AggregateReport(
            data=data["df_aggregate"], v1=v1, v2=v2, v3=v3, condo=condo, mapped=mapped
        )()

        NullReport(
            data=data["df_null"], v1=v1, v2=v2, v3=v3, condo=condo, mapped=mapped
        )()

        SourceDataVersionsReport(version_text=data["version_text"])()

        ExpectedValueDifferencesReport(data=data["df_expected"], v1=v1, v2=v2)()

        OutlierReport(
            data=data["df_outlier"], v1=v1, v2=v2, condo=condo, mapped=mapped
        )()

    if report_type == "Compare with Previous Version":
        version_comparison_report(data)
    elif report_type == "Review Manual Changes":
        ChangesReport(data)()
