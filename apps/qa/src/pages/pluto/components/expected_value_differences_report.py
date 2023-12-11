import streamlit as st
import pandas as pd

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

fields_of_interest_dict = {
    "zoning": [ZONING_FIELDS],
    "special district": [SPECIAL_DISTRICT_FIELDS],
    "commericial overlay": [COMMERCIAL_OVERLAY_FIELDS],
} | {field: [[field]] for field in OTHER_FIELDS}


FIELDS_OF_INTEREST = pd.DataFrame.from_dict(
    fields_of_interest_dict,
    orient="index",
    columns=["relevant fields"],
)
FIELDS_OF_INTEREST.index.names = ["field group name"]


class ExpectedValueDifferencesReport:
    def __init__(self, data, v1, v2):
        self.df = data
        self.v1 = v1
        self.v2 = v2

    def __call__(self):
        st.header("Expected Value Comparison")
        st.markdown(
            f"""
            For some fields, we document the expected values and their descriptions in PLUTO documentation.
            Therefore, it's important for us to know when new values are added to a field or if a value is no longer present in that field.

            For these checks, we group certain fields together. For example, we want to know if there are new values for zoning districts, not `zonedist1` in particular.
            """
        )
        st.markdown(f"### Grouped fields of interest")
        st.dataframe(
            FIELDS_OF_INTEREST,
            column_config={
                "relevant fields": st.column_config.ListColumn(
                    width="large",
                ),
            },
        )

        for comparison_name in FIELDS_OF_INTEREST.index:
            st.markdown(f"#### Value differences for `{comparison_name}`")

            in1not2, in2not1 = self.value_differences_across_versions(comparison_name)
            value_differences = pd.DataFrame.from_dict(
                dict(
                    [
                        (f"in {self.v1} but not {self.v2}", [in1not2]),
                        (f"in {self.v2} but not {self.v1}", [in2not1]),
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
        return self.df[self.df["v"].isin([self.v1, self.v2])].to_dict("records")

    @property
    def v1_expected_records(self) -> dict:
        return self.expected_records_by_version(self.v1)

    @property
    def v2_expected_records(self) -> dict:
        return self.expected_records_by_version(self.v2)

    def expected_records_by_version(self, version) -> dict:
        return [i["expected"] for i in self.expected_records if i["v"] == version][0]

    def values_by_field(self, df: dict, field: str) -> list:
        return [i["values"] for i in df if i["field"] == field][0]

    def values_by_fields(self, df: dict, fields: list[str]) -> list:
        values: list = []
        for field in fields:
            values = values + self.values_by_field(df, field)
        return values

    def value_differences(self, df1, df2) -> list:
        return [i for i in df1 if i not in df2]

    def value_differences_across_versions(
        self, comparison_name: str
    ) -> tuple[list, list]:
        v1_values = self.values_by_fields(
            self.v1_expected_records,
            FIELDS_OF_INTEREST.loc[comparison_name]["relevant fields"],
        )
        v2_values = self.values_by_fields(
            self.v2_expected_records,
            FIELDS_OF_INTEREST.loc[comparison_name]["relevant fields"],
        )
        in1not2 = self.value_differences(v1_values, v2_values)
        in2not1 = self.value_differences(v2_values, v1_values)
        return (in1not2, in2not1)
