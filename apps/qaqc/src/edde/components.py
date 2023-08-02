import streamlit as st
from src.edde.helpers import compare_columns


def column_comparison_table(old_data, new_data):
    dropped, added, union, paired = compare_columns(old_data, new_data, True)
    nrows_mismatch = max(len(dropped), len(added))

    with st.expander(f"{len(union)} matching columns"):
        for col in union:
            st.info(col)

    with st.expander(f"{len(paired)} columns with updated year in header"):
        col1, col2 = st.columns(2)
        col1.write("Old")
        col2.write("Updated")

        for old_column, matched_columns in paired:
            with col1:
                st.warning(old_column)
            with col2:
                st.warning(", ".join(matched_columns))

    with st.expander(f"{len(dropped)} columns dropped, {len(added)} columns added"):
        col1, col2 = st.columns(2)
        col1.write("Dropped")
        col2.write("Added")

        for i in range(nrows_mismatch):
            with col1:
                if i < len(dropped):
                    st.error(dropped[i])
            with col2:
                if i < len(added):
                    st.success(added[i])
