import plotly.graph_objects as go
import streamlit as st
from numerize.numerize import numerize
from src.shared.constants import COLOR_SCHEME


class AggregateReport:
    def __init__(self, data, v, v_prev, v_comp, v_comp_prev, condo, mapped):
        self.df = data
        self.v = v
        self.v_prev = v_prev
        self.v_comp = v_comp
        self.v_comp_prev = v_comp_prev
        self.condo = condo
        self.mapped = mapped

    def __call__(self):
        st.header("Aggregate Changes")

        st.info(self.info_description)

        self.display_graph()

        st.write(self.aggregate_df.sort_values(by="v", ascending=False))

    @property
    def aggregate_df(self):
        return self.df.loc[
            (self.df.condo == self.condo)
            & (self.df.mapped == self.mapped)
            & (self.df.v.isin([self.v, self.v_prev, self.v_comp, self.v_comp_prev])),
            :,
        ]

    @property
    def v_records(self):
        return self.records_by_version(self.v)

    @property
    def v_prev_records(self):
        return self.records_by_version(self.v_prev)

    @property
    def v_comp_records(self):
        return self.records_by_version(self.v_comp)

    @property
    def v_comp_prev_records(self):
        return self.records_by_version(self.v_comp_prev)

    def records_by_version(self, version):
        return self.aggregate_df.loc[self.aggregate_df.v == version, :].to_dict(
            "records"
        )[0]

    def x_columns(self, v1, v2):
        return [(v1[i] / v2[i] - 1) * 100 for i in self.y_columns]

    def differences(self, v1, v2):
        return [v1[i] - v2[i] for i in self.y_columns]

    def version_values(self, version):
        return [version[i] for i in self.y_columns]

    def version_pair(self, v1, v2):
        return f"{v1['v']} - {v2['v']}"

    def generate_trace(self, v1, v2):
        differences = self.differences(v1, v2)
        real_v1 = self.version_values(v1)
        real_v2 = self.version_values(v2)

        hovertemplate = "<b>%{x}</b> %{text}"
        text = []
        for n in range(len(self.y_columns)):
            text.append(
                "Diff: {} | Current: {} | Prev: {}".format(
                    numerize(differences[n]),
                    numerize(real_v1[n]),
                    numerize(real_v2[n]),
                )
            )

        return go.Scatter(
            x=self.y_columns,
            y=self.x_columns(v1, v2),
            mode="lines",
            name=self.version_pair(v1, v2),
            hovertemplate=hovertemplate,
            text=text,
        )

    def display_graph(self):
        fig = go.Figure()

        fig.add_trace(self.generate_trace(self.v_records, self.v_prev_records))
        fig.add_trace(
            self.generate_trace(self.v_comp_records, self.v_comp_prev_records)
        )

        fig.add_hline(y=0, line_color="grey", opacity=0.5)
        fig.update_layout(
            title="Percent change in sum of columns for all PLUTO lots",
            template="plotly_white",
            yaxis={"title": "Percent Change"},
            colorway=COLOR_SCHEME,
        )

        st.plotly_chart(fig)

    @property
    def info_description(self):
        return """
            In addition to looking at the number of lots with a changed value, it’s important to look at the magnitude of the change, summed across the entire dataset.
            For example, the mismatch graph for finance may show that over 90% of lots get an updated assessment when the tentative roll is released. 
            The aggregate graph may show that the aggregated sum increased by 5%. 
            Totals for assessland, assesstot, and exempttot should only change after the tentative and final rolls. 
            Pay attention to any large changes to residential units (unitsres).
            The graph shows the percent increase or decrease in the sum of the field between version. 
            The table reports the raw numbers for more in depth analysis.
        """

    @property
    def y_columns(self):
        return [
            "unitsres",
            "lotarea",
            "bldgarea",
            "comarea",
            "resarea",
            "officearea",
            "retailarea",
            "garagearea",
            "strgearea",
            "factryarea",
            "otherarea",
            "assessland",
            "assesstot",
            "exempttot",
            "firm07_flag",
            "pfirm15_flag",
        ]
