import pandas as pd
import streamlit as st

ZONING_FIELDS = [
    "zonedist1",
    "zonedist2",
    "zonedist3",
    "zonedist4",
]
SPECIAL_DISTRICT_FIELDS = [
    "spdist1",
    "spdist2",
    "spdist3",
]
COMMERCIAL_OVERLAY_FIELDS = [
    "overlay1",
    "overlay2",
]
OTHER_FIELDS = [
    "ext",
    "proxcode",
    "irrlotcode",
    "lottype",
    "bsmtcode",
    "bldgclasslanduse",
]

FIELDS_OF_INTEREST = {
    "zoning": ZONING_FIELDS,
    "special district": SPECIAL_DISTRICT_FIELDS,
    "commericial overlay": COMMERCIAL_OVERLAY_FIELDS,
} | {field: [field] for field in OTHER_FIELDS}


class ExpectedValueDifferencesReport:
    def __init__(self, data, v, v_prev):
        self.df = data
        self.v = v
        self.v_prev = v_prev

    def __call__(self):
        st.header("Expected Value Comparison")
        st.markdown(
            """
            For some fields, we document the expected values and their descriptions in PLUTO documentation.
            Therefore, it's important for us to know when new values are added to a field or if a value is no longer present in that field.

            For these checks, we group certain fields together. For example, we want to know if there are new values for zoning districts, not `zonedist1` in particular.
            """
        )
        st.markdown("### Grouped fields of interest")
        fields_of_interest = pd.DataFrame.from_dict(
            {f: [v] for f, v in FIELDS_OF_INTEREST.items()},
            orient="index",
            columns=["relevant fields"],
        )
        fields_of_interest.index.names = ["field group name"]
        st.dataframe(
            fields_of_interest,
            column_config={
                "relevant fields": st.column_config.ListColumn(
                    width="large",
                ),
            },
        )

        for comparison_name in FIELDS_OF_INTEREST:
            st.markdown(f"#### Value differences for `{comparison_name}`")

            in1not2, in2not1 = self.value_differences_across_versions(comparison_name)
            value_differences = pd.DataFrame.from_dict(
                dict(
                    [
                        (f"in {self.v} but not {self.v_prev}", [in1not2]),
                        (f"in {self.v_prev} but not {self.v}", [in2not1]),
                    ]
                ),
                orient="index",
                columns=["values"],
            )

            if len(in1not2) == 0 and len(in2not1) == 0:
                st.write("None")
            else:
                st.dataframe(
                    value_differences,
                    column_config={
                        "relevant fields": st.column_config.ListColumn(
                            width="large",
                        ),
                    },
                )

    @property
    def expected_records(self) -> dict:
        return self.df[self.df["v"].isin([self.v, self.v_prev])].to_dict("records")

    @property
    def v_expected_records(self) -> dict:
        return self.expected_records_by_version(self.v)

    @property
    def v_prev_expected_records(self) -> dict:
        return self.expected_records_by_version(self.v_prev)

    def expected_records_by_version(self, version) -> dict:
        return [i["expected"] for i in self.expected_records if i["v"] == version][0]

    def values_by_field(self, df: dict, field: str) -> list:
        return [i["values"] for i in df if i["field"] == field][0]

    def values_by_fields(self, df: dict, fields: list[str]) -> list:
        values: set = set()
        for field in fields:
            values.update(self.values_by_field(df, field))
        return list(values)

    def value_differences(self, df1, df2) -> list:
        return sorted([i for i in df1 if i not in df2])

    def value_differences_across_versions(
        self, comparison_name: str
    ) -> tuple[list, list]:
        v_values = self.values_by_fields(
            self.v_expected_records,
            FIELDS_OF_INTEREST[comparison_name],
        )
        v_prev_values = self.values_by_fields(
            self.v_prev_expected_records,
            FIELDS_OF_INTEREST[comparison_name],
        )
        in1not2 = self.value_differences(v_values, v_prev_values)
        in2not1 = self.value_differences(v_prev_values, v_values)
        return (in1not2, in2not1)
