def checkbook():
    import streamlit as st
    from src.shared.components import build_outputs, sidebar
    from src.shared.utils import publishing

    from .components import output_map

    st.title("Capital Spending Database")

    agency = st.sidebar.selectbox(
        "Agency Selection: ",
        (
            "All",
            "SCHOOL CONSTRUCTION AUTHORITY",
            "Department of Education",
            "Transit Authority",
            "Department of Environmental Protection",
            "Housing, Preservation and Development",
            "Department of Parks and Recreation",
            "Department of Citywide Administrative Services",
            "Health and Hospitals Corporation",
        ),
    )

    product_key = sidebar.data_selection("db-checkbook")
    if not product_key:
        st.header("Select a version.")
    else:
        build_outputs.data_directory_link(product_key)

        st.markdown(
            body="""
            ### About

            The [Capital Spending Database](https://github.com/NYCPlanning/data-engineering/tree/main/products/checkbook) (db-checkbook), a data product produced by the New York City (NYC) Department of City Planning (DCP) Data Engineering team, presents the first-ever spatialized view of historical liquidations of the NYC capital budget. 
            Each row in the dataset corresponds to a unique capital project and captures vital information such as the sum of all checks disbursed for the project, a list of agencies associated with the project, a category assignment based on keywords in project text fields (‘Fixed Asset’, ‘Lump Sum’, or ‘ITT, Vehicles, and Equipment’), and most importantly, geospatial information associated with the project when possible. 
            Currently, the dataset is created from two source datasets: Checkbook NYC and the Capital Projects Database (CPDB). Checkbook NYC provides the information related to historical liquidations of the capital budget at the capital project level, while CPDB provides geospatial information for those projects. 
            Checkbook NYC is an open-source dataset and tool from the NYC Comptroller’s Office that publishes every check disbursed by the city. For the Historical Liquidations dataset, we limited the scope of this data source to only those checks pertaining to capital spending, as defined by Checkbook NYC. 
            CPDB is a data product produced by the NYC DCP Data Engineering team that “captures key data points on potential, planned, and ongoing capital projects sponsored or maintained by a capital agency in and around NYC” [source]. CPDB is updated three times per year in response to the Capital Commitment Plan (CCP). 
            Because only capital projects reported in the current fiscal year (FY)’s CCP are reflected in CPDB, the Historical Liquidations dataset utilizes the previous adopted versions of CPDB, in addition to the most recent version of CPDB from the current FY. 
            """
        )

        data = publishing.read_csv_cached(product_key, "historical_spend.csv")

        if agency != "All":
            agency_data = data[data["agency"].str.contains(agency)]
        else:
            agency_data = data

        grouped_by_category = agency_data.groupby("final_category", as_index=False).sum(
            numeric_only=True
        )
        grouped_by_geometry = agency_data.groupby("has_geometry", as_index=False).sum(
            numeric_only=True
        )

        st.header("Check Amounts Per Category")
        st.bar_chart(grouped_by_category, x="final_category", y="check_amount")

        st.header("Check Amounts Represented by Geometries")
        st.bar_chart(grouped_by_geometry, x="has_geometry", y="check_amount")

        output_map(data)
