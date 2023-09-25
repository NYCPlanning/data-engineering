def source_data():
    import streamlit as st
    from src.source_data import helpers, components

    product = st.sidebar.selectbox("Select a product", ["PLUTO"])
    datasets = helpers.get_dataset_versions(product)

    months_back = st.sidebar.number_input("Months to display", min_value=1, value=3)

    plot = components.plot_series(datasets, months_back)
