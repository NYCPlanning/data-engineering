def source_data():
    import streamlit as st

    from . import components, helpers

    product = st.sidebar.selectbox(
        "Select a product",
        [
            "cbbr",
            "colp",
            "cpdb",
            "developments",
            "facilities",
            "pluto",
            "zoningtaxlots",
        ],
    )

    datasets = helpers.get_dataset_metadata(product)

    months_back = st.sidebar.number_input("Months to display", min_value=1, value=6)

    st.sidebar.button(
        label="Scrape source metadata",
        help=f"Scrape spaces for all versions, timestamps of all source datasets of {product}",
        on_click=lambda: helpers.scrape_metadata(product),
    )

    components.plot_series(datasets, months_back)
