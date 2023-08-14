import streamlit as st
from st_aggrid import AgGrid


class ManualCorrection:
    def __init__(self, data):
        self.modifications_applied = data["modifications_applied"]
        self.modifications_not_applied = data["modifications_not_applied"]

    def __call__(self):
        st.subheader("Manual Correction Report")
        self.display_applied_correction(self.modifications_applied)
        self.display_not_applied_correction(self.modifications_not_applied)

    def display_applied_correction(self, df):
        if df is None:
            st.write("Manual correction table for applied records is not available.")
            return
        if not df.empty:
            count1 = df.groupby("field").size()
            count1 = count1.reset_index()
            count1.rename(
                columns={"field": "Field", 0: "Number of Records"}, inplace=True
            )
            st.subheader("Numbers of Manual Corrections Applied By Field")
            st.write(count1)
        else:
            st.write("Manual correction table for applied records is empty.")

    def display_not_applied_correction(self, df):
        if df is None:
            st.write(
                "Manual correction table for not applied records is not available."
            )
            return
        if not df.empty:
            count2 = df.groupby("field").size()
            count2 = count2.reset_index()
            count2.rename(
                columns={"field": "Field", 0: "Number of Records"}, inplace=True
            )
            st.subheader("Numbers of Manual Corrections Not Applied By Field")
            st.write(count2)
            df = df.sort_values(by=["field"])
            df = df[df.columns.tolist()[1:] + ["uid"]]
            st.subheader("Table of Manual Corrections Not Applied")
            AgGrid(df)
        else:
            st.write("Manual correction table for not applied records is empty.")
