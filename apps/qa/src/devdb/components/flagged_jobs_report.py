import streamlit as st


class FlaggedJobsReport:
    def __init__(self, data, qaqc_check_dict) -> None:
        self.qaqc_app = data["qaqc_app"]
        self.qaqc_checks = qaqc_check_dict

    def __call__(self):
        st.subheader("Flagged Jobs by QAQC Check")
        st.markdown(
            "Each of these tables lists job numbers with specific highlighted potential issues."
        )

        if self.qaqc_app is None:
            st.info("QAQC Flags file missing for this branch.")
            return

        qaqc_check = st.selectbox(
            "Choose a QAQC Check to View Flagged Records",
            options=self.qaqc_checks.keys(),
        )

        self.display_check(qaqc_check)

    def display_check(self, qaqc_check):
        st.markdown(self.qaqc_checks[qaqc_check]["description"])
        df = self.filter_by_check(qaqc_check)

        if df.empty:
            st.write("There are no jobs with this status.")
        else:
            st.write(df)

    def filter_by_check(self, check):
        if self.qaqc_checks[check]["field_type"] == "boolean":
            return self.qaqc_app.loc[self.qaqc_app[check] == 1][["job_number"]]
        elif self.qaqc_checks[check]["field_type"] == "string":
            return self.qaqc_app.loc[self.qaqc_app[check].notnull()][
                ["job_number", check]
            ]
