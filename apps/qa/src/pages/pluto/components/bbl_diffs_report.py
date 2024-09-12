import streamlit as st


class BblDiffsReport:
    def __init__(self, data):
        self.df_bbl_diffs = data

    def __call__(self):
        st.header("New and Vanished BBLs")
        st.markdown(
            """
            The table of BBLs in the current PLUTO that were added (new) and removed (vanished) since its previous version. 
            """
        )
        st.info(
            """
            ⚠️ The filters on the left to filter condo and mapped lots DO NOT apply to this table. 
            """
        )
        st.info(
            """
            ⚠️ When choosing a PLUTO minor version, the table below should be empty: BBL records are supposed to be added/removed in major versions only.
            """
        )

        if self.df_bbl_diffs is None:
            st.warning("QAQC table for vanished and new BBLs was not found.")
            return

        df = self.df_bbl_diffs
        st.write(df)
