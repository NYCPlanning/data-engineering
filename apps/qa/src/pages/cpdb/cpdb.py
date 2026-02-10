def cpdb():
    import plotly.express as px
    import plotly.graph_objects as go
    import streamlit as st
    from src.shared.components import build_outputs, sidebar
    from src.shared.constants import COLOR_SCHEME

    from .components.adminbounds import adminbounds
    from .components.geometry_visualization_report import (
        geometry_visualization_report,
    )
    from .components.withinNYC_check import withinNYC_check
    from .helpers import (
        PRODUCT,
        VIZKEY,
        get_commit_cols,
        get_data,
        get_diff_dataframe,
        get_map_percent_diff,
        sort_base_on_option,
    )

    st.title("Capital Projects Database QAQC")

    staging_product_key = sidebar.data_selection(PRODUCT, "Choose a dataset for qa")

    reference_product_key = sidebar.data_selection(
        PRODUCT, "Choose a reference dataset"
    )
    agency_label = {"sagency": "Sponsoring Agency", "magency": "Managing Agency"}
    agency_type = st.sidebar.selectbox(
        "select an agency type",
        ["sagency", "magency"],
        format_func=lambda x: agency_label.get(x),
    )
    agency_type_title = agency_label[agency_type]

    view_type = st.sidebar.selectbox(
        "select to view by number of projects or values of commitments in dollars",
        ["projects", "commitments"],
    )
    view_type_title = view_type.capitalize()
    view_type_unit = (
        "Number of Projects" if view_type == "projects" else "Commitments Amount (USD)"
    )

    subcategory = st.sidebar.selectbox(
        "choose a subcategory or entire portfolio", ["all categories", "fixed assets"]
    )

    build_outputs.data_directory_link(staging_product_key)

    st.markdown(
        body="""
        
        ### About the Capital Projects Database

        The Capital Projects Database (CPDB), a data product produced by the New York City (NYC) Department of City Planning (DCP) Data Engineering team, captures key data points on potential, planned, and ongoing capital projects sponsored or managed by a capital agency in and around NYC.
        Information reported in the Capital Commitment Plan published by the NYC Office of Management and Budget (OMB) three times per year is the foundation that CPDB is built from.  Therefore, only capital projects that are in the Capital Commitment Plan are reflected in CPDB. Additional data sources are incorporated to map the capital projects.
        CPDB enables Planners to better understand and communicate New York City's capital project portfolio within and across particular agencies. While not comprehensive, CPDB's spatial data provides a broad understanding of what projects are taking place within a certain area, and is starting point to discovering opportunities for strategic neighborhood planning.

        ### About the QAQC Reports

        The QAQC page is designed to highlight key measures that can indicate potential data issues in a CPDB build. These graphs report summary statistics at the agency level and there are 3 ways to filter and view the data (w/ additional variation at the graph level):

        1. Agency type: Sponsoring agency OR Managing agency
        2. Aggregation type: the total number of projects OR the total sum ($) of all commitments
        3. Category type: Include projects in all categories (fixed asset, lump sum, ITT, Vehicles & equipment) OR include only projects that are categorized as Fixed Asset

        Additionally, there are basic geographic checks to facilitate the QAQC process of the disparate source data we receive from various city agencies. These checks are not meant to be comprehensive, rather they are intended to provide an indication if spatial data is outside of the NYC spatial boundaries or incorrect in some way.

        ### Key CPDB QAQC terms: 

        - **Mapped**: refers to a project record that has a geometry / spatial data associated with it.
        - Agency type: The **Managing** agency is the NYC Agency that is overseeing the construction of a project.  The **Sponsoring** agency is the NYC Agency that is funding the project.  The managing agency and sponsoring agency can be, but are not always the same.
        - Category type: Fixed Assets, are projects that are place specific and have an impact on the surrounding area, visible or not, such as park improvements or sewer reconstruction.  Projects are categorized by DCP based on key works in the project description.  Other categories include Lump Sum, and ITT, Vehicles, and Equipment​.
        - A **project** is a discrete capital investment, and is defined as a record that has a unique FMS ID.  
        - A **commitment** is an individual contribution to fund a portion of a project.  When looking at the "commitment" view you're looking at the sum of a commitments.
        
        #### Additional Resources
        - [CPDB Github Repo Wiki Page](https://github.com/NYCPlanning/db-cpdb/wiki) 
        - [Medium Blog on CPDB](https://medium.com/nyc-planning-digital/welcome-to-the-world-dcps-capital-projects-database-693a8b9782ac)

        """
    )

    if not (staging_product_key and reference_product_key):
        st.header("Select a version.")
    else:
        data = get_data(staging_product_key, reference_product_key)
        df = data["cpdb_summarystats_" + agency_type].set_index(agency_type + "acro")
        df_pre = data["pre_cpdb_summarystats_" + agency_type].set_index(
            agency_type + "acro"
        )
        if view_type == "commitments":
            st.header(
                f"Dollar ($) Value of Commitments by {agency_type_title} for {subcategory} (Mapped vs Unmapped)"
            )
            df = df[get_commit_cols(df)]
            df_pre = df_pre[get_commit_cols(df_pre)]
        else:
            st.header(
                f"Number of Projects by {agency_type_title} for {subcategory} (Mapped vs Unmapped)"
            )
            df.drop(labels=get_commit_cols(df), axis=1, inplace=True)
            df_pre.drop(labels=get_commit_cols(df_pre), axis=1, inplace=True)

        # sort the values based on projects/commitments and get the top ten agencies
        df_bar = sort_base_on_option(
            df, subcategory, view_type, map_option=0, ascending=False
        )
        # print(df_bar.index)
        fig1 = px.bar(
            df_bar,
            x=df_bar.index,
            y=VIZKEY[subcategory][view_type]["values"],
            labels=dict(sagencyacro="Sponsoring Agency", magencyacro="Managing Agency"),
            barmode="group",
            width=1000,
            color_discrete_sequence=COLOR_SCHEME,
        )

        fig1.update_yaxes(title=view_type_unit)

        fig1.update_layout(legend_title_text="Variable")

        st.plotly_chart(fig1)

        st.caption(
            f"""This graph reports the {view_type_unit} (both mapped and unmapped) by {agency_type_title} for {subcategory}. 
            Typically, large city agencies including DPR (Dept. Parks and Rec.), DEP (Dept. of Environmental Protection), DOT (Dept. of Transportation), and DCAS (Dept of Citywide Admin. Services) have the largest count of projects and, generally, the highest capital expenditure.
            Some agencies (e.g. HPD [Housing Preservation & Development]) often have fewer total projects but high capital expenditure because of the nature of their projects which are related to building housing across NYC.
            The purpose of this graph is to get an overview of the distribution of projets or commitments by agency, and a sense of what portion of these are mapped."""
        )

        # ----- 2nd Graph
        st.header(
            f"Compare the Total {view_type_unit} in the Previous Version vs. the Latest Version of CPDB by {agency_type_title}"
        )

        map_options = {0: f"all {view_type}", 1: f"mapped {view_type} only"}
        map_option = st.radio(
            label=f"Choose to compare either all {view_type} or mapped {view_type} only.",
            options=[0, 1],
            format_func=lambda x: map_options.get(x),
        )
        map_title_text = "Mapped and Unmapped" if map_option == 0 else "Mapped Only"
        # get the difference dataframe
        diff = get_diff_dataframe(df, df_pre)
        df_bar_diff = sort_base_on_option(
            diff, subcategory, view_type, map_option=map_option
        )
        fig2 = go.Figure(
            [
                go.Bar(
                    name="Difference",
                    x=df_bar_diff[VIZKEY[subcategory][view_type]["values"][map_option]],
                    y=df_bar_diff.index,
                    orientation="h",
                ),
                go.Bar(
                    name="Latest Version",
                    x=df[VIZKEY[subcategory][view_type]["values"][map_option]],
                    y=df.index,
                    orientation="h",
                    visible="legendonly",
                ),
                go.Bar(
                    name="Previous Version",
                    x=df_pre[VIZKEY[subcategory][view_type]["values"][map_option]],
                    y=df_pre.index,
                    orientation="h",
                    visible="legendonly",
                ),
            ]
        )
        fig2.update_layout(
            barmode="group",
            width=1000,
            height=1000,
            title_text=f"Total {view_type_unit} by Version and {agency_type_title} ({map_title_text})",
            colorway=COLOR_SCHEME,
        )

        fig2.update_xaxes(title=f"Total {view_type_unit} ({map_title_text})")

        fig2.update_yaxes(title=agency_type_title)

        st.plotly_chart(fig2)

        st.caption(
            f"""  
            This graph visualizes the difference in the {view_type_unit} by {agency_type_title} between the current (aka latest) and the previous version of CPDB. 
            While the underlying Capital Commitment Plan data changes between versions, any drastic changes between CPDB versions that are illustrated by this graph can indicate if there is a specific agency or source dataset to look into further that may have introduced these anomalies.
            Anomalies include, but are not limited to, no projects being mapped for a given agency when there were mapped projects in the previous version, the number of projects doubling for an agency between versions, or the total sum of commitments halving for an agency between versions.
            This chart also gives the viewer the flexibility to change between all projects by Number of Projects (both mapped and unmapped) along with an option to just view the mapped (geolocated) projects. Click the "Latest Version" and "Previous Version" labels in the legend to display the total Number of Projects for each.
            """
        )

        #### ----- 3rd Graph
        st.header(
            f"Compare Mapping of {view_type.capitalize()} between Previous and Latest Versions by {agency_type_title}"
        )

        diff_perc = get_map_percent_diff(df, df_pre, VIZKEY[subcategory][view_type])

        fig3 = go.Figure(
            [
                go.Bar(
                    name="Difference",
                    x=diff_perc.diff_percent_mapped,
                    y=diff_perc.index,
                    orientation="h",
                ),
                go.Bar(
                    name="Latest Version",
                    x=diff_perc.percent_mapped,
                    y=diff_perc.index,
                    orientation="h",
                    visible="legendonly",
                ),
                go.Bar(
                    name="Previous Version",
                    x=diff_perc.pre_percent_mapped,
                    y=diff_perc.index,
                    orientation="h",
                    visible="legendonly",
                ),
            ]
        )

        fig3.update_layout(
            width=1000,
            height=1000,
            title_text=f"Percentage of {view_type_title} Mapped by Version and {agency_type_title}",
            colorway=COLOR_SCHEME,
        )

        fig3.update_xaxes(title="Percentage", tickformat=".2%")
        fig3.update_yaxes(title=agency_type_title)
        st.plotly_chart(fig3)

        st.caption(
            f"""
            This graph shows another important cut of the data in which we higlight the percentage of {view_type} succesfully mapped (geocoded) by {agency_type_title} between the last two verions of CPDB along with the pct. difference between those verions. 
            Typically, we'd expect a similar pct. of records to be mapped by {agency_type_title} between versions and any significant change should be looked at more closely.
            Click the "Latest Version" and "Previous Version" labels in the legend to display the percentage mapped for each.
            
            """
        )

        adminbounds(data)

        withinNYC_check(data)

        geometry_visualization_report(data)
