import streamlit as st
from dcpy.connectors.edm import publishing


def data_directory_link(product_key: publishing.ProductKey):
    data_url = publishing.get_data_directory_url(product_key)
    st.markdown(
        f"""
        [Link]({data_url}) to the dataset in Digital Ocean.        
        *Please let the DE team know if the link doesn't work.*
    """
    )
