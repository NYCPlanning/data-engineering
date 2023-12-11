import streamlit as st


class OutlierReport:
    def __init__(self, data):
        self.ipis_cd_errors = data["ipis_cd_errors"]

    def __call__(self):
        st.subheader("Outlier Report")
        st.markdown(
            """
            This section shows mismatch between IPIS community district and PLUTO.
            """
        )
        if self.ipis_cd_errors is None:
            st.info(
                "No QAQC table for mismatch between IPIS community district and PLUTO."
            )
            return
        self.display_ipis_mismatch()

    def display_ipis_mismatch(self):
        st.markdown("#### Mismatch between IPIS Community District and PLUTO")
        df = self.ipis_cd_errors[~self.ipis_cd_errors["pluto_cd"].isnull()]
        df["BBL"] = df["BBL"].astype(int)
        df["pluto_cd"] = df["pluto_cd"].astype(int)
        df = df.drop(columns=["uid"])
        st.write(df)
