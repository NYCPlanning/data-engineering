import plotly.graph_objects as go
import streamlit as st
from src.shared.constants import COLOR_SCHEME


class MismatchReport:
    def __init__(self, data, v, v_prev, v_comp, v_comp_prev, condo, mapped):
        self.df_mismatch = data
        self.v = v
        self.v_prev = v_prev
        self.v_comp = v_comp
        self.v_comp_prev = v_comp_prev
        self.version_pairs = [
            f"{v} - {v_prev}",
            f"{v_comp} - {v_comp_prev}",
        ]
        self.condo = condo
        self.mapped = mapped

    def __call__(self):
        df = self.filter_by_options()
        v1v2 = self.filter_by_version_pair(df, self.version_pairs[0])
        v2v3 = self.filter_by_version_pair(df, self.version_pairs[1])

        st.header("Mismatched Records")
        st.info(
            "Each mismatch graph and table shows the number of records (identified by bbl) with a changed value in a given field."
        )

        for group in self.groups:
            self.display_graph(v1v2, v2v3, group)

        st.subheader("Summary of Differences by Field")
        st.write(df)
        st.info(
            """
            This table reports the number of records with differences in a field value between versions. 
            This table is useful for digging into any anomalies identified using the graphs above.
            """
        )

    def display_graph(self, v1v2, v2v3, group):
        fig = go.Figure()

        fig.add_trace(self.generate_version_trace(v1v2, group["columns"]))
        fig.add_trace(self.generate_version_trace(v2v3, group["columns"]))

        fig.update_layout(
            title=group["title"],
            template="plotly_white",
            colorway=COLOR_SCHEME,
            yaxis={"title": "# Changed Records"},
        )

        st.plotly_chart(fig)
        st.info(group["description"])

    def generate_version_trace(self, df, columns):
        group_df = self.filter_by_columns(df, columns)

        hovertemplate = "<b>%{x} %{text}</b>"

        return go.Scatter(
            x=group_df.index,
            y=group_df.totals,
            mode="lines",
            name=df["pair"].iloc[0],
            hovertemplate=hovertemplate,
            text=group_df.text,
        )

    def filter_by_options(self):
        return self.df_mismatch.loc[
            (self.df_mismatch.condo == self.condo)
            & (self.df_mismatch.mapped == self.mapped)
            & (self.df_mismatch.pair.isin(self.version_pairs)),
            :,
        ]

    def filter_by_version_pair(self, df, version_pair):
        version_pair_data = df.loc[df.pair == version_pair, :]
        if len(version_pair_data) == 0:
            valid_pairs_text = "  \n".join(
                sorted(list(self.df_mismatch.pair.unique()), reverse=True)
            )
            st.error(
                f"""
                Valid PLUTO qaqc_mismatch version pairs for chosen S3 build folder:  
                {valid_pairs_text}
                """
            )
            raise ValueError(
                f"""No data found in qaqc_mismatch.csv for version pair {version_pair},
                likely because QAQC data is only generated during PLUTO builds and
                not dynamically in this app."""
            )
        return version_pair_data

    def total(self, df):
        return df["total"].iloc[0]

    def filter_by_columns(self, df, columns):
        new_df = df[columns].iloc[0].to_frame("totals")
        new_df["text"] = round(new_df["totals"] / self.total(df) * 100, 2)

        return new_df

    @property
    def groups(self):
        return [
            {
                "title": "Mismatch graph -- finance fields",
                "columns": self.finance_columns,
                "description": """
                    DOF updates the assessment and exempt values twice a year. 
                    The tentative tax roll is released in mid-January and the final tax roll is released in late May. 
                    We expect the values of the fields in the above graph to change in versions of PLUTO created after the release of the tentative or final roll. 
                    For the PLUTO version created right after the tentative roll, most lots will show a change in assesstot, with a smaller number of changes for the assessland and exempttot.
                    There will also be changes to these fields in the version created after the release of the final roll. 
                    Versions created between roll releases should see almost no change for these fields.
                """,
            },
            {
                "title": "Mismatch graph -- area fields",
                "columns": self.area_columns,
                "description": """
                    CAMA is the primary source for the area fields. Updates reflect new construction, as well as updates by assessors for the tentative roll. 
                    Several thousand lots may have updates in the version created after the tentative tax roll.
                """,
            },
            {
                "title": "Mismatch graph -- zoning fields",
                "columns": self.zoning_columns,
                "description": """
                Unless DCP does a major rezoning, the number of lots with changed values should be **no more than a couple of hundred**.
                Lots may get a changed value due to a split/merge or if TRD is cleaning up boundaries between zoning districts.
                `Residfar`, `commfar`, and `facilfar` should change only when there is a change to `zonedist1` or `overlay1`.
            """,
            },
            {
                "title": "Mismatch graph -- geo fields",
                "columns": self.geo_columns,
                "description": """
                These fields are updated from **Geosupport**. Changes should be minimal unless a municipal service
                area changes or more high-rise buildings opt into the composite recycling program.
                Check with GRU if more than a small number of lots have changes to municipal service areas.
            """,
            },
            {
                "title": "Mismatch graph -- building fields",
                "columns": self.bldg_columns,
                "description": """
                    Changes in these fields are most common after the tentative roll has been released. 
                    Several fields in this group are changed by DCP to improve data quality, including ownername and yearbuilt. 
                    When these changes are first applied, there will be a spike in the number of lots changed.
                """,
            },
        ]

    @property
    def finance_columns(self):
        return [
            "assessland",
            "assesstot",
            "exempttot",
            "taxmap",
            "appbbl",
            "appdate",
            "plutomapid",
        ]

    @property
    def area_columns(self):
        return [
            "lotarea",
            "bldgarea",
            "builtfar",
            "comarea",
            "resarea",
            "officearea",
            "retailarea",
            "garagearea",
            "strgearea",
            "factryarea",
            "otherarea",
            "areasource",
        ]

    @property
    def zoning_columns(self):
        return [
            "residfar",
            "commfar",
            "facilfar",
            "zonedist1",
            "zonedist2",
            "zonedist3",
            "zonedist4",
            "overlay1",
            "overlay2",
            "spdist1",
            "spdist2",
            "spdist3",
            "ltdheight",
            "splitzone",
            "zonemap",
            "zmcode",
            "edesignum",
        ]

    @property
    def geo_columns(self):
        return [
            "cd",
            # "bct2020",
            # "bctcb2020",
            "ct2010",
            "cb2010",
            "schooldist",
            "council",
            "zipcode",
            "firecomp",
            "policeprct",
            "healtharea",
            "sanitboro",
            "sanitsub",
            "address",
            "borocode",
            "bbl",
            "tract2010",
            "xcoord",
            "ycoord",
            "longitude",
            "latitude",
            "sanborn",
            "edesignum",
            "sanitdistrict",
            "healthcenterdistrict",
            "histdist",
            "firm07_flag",
            "pfirm15_flag",
        ]

    @property
    def bldg_columns(self):
        return [
            "bldgclass",
            "landuse",
            "easements",
            "ownertype",
            "ownername",
            "numbldgs",
            "numfloors",
            "unitsres",
            "unitstotal",
            "lotfront",
            "lotdepth",
            "bldgfront",
            "bldgdepth",
            "ext",
            "proxcode",
            "irrlotcode",
            "lottype",
            "bsmtcode",
            "yearbuilt",
            "yearalter1",
            "yearalter2",
            "landmark",
            "condono",
        ]
