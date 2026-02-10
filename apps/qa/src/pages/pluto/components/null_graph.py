import plotly.express as px
import streamlit as st
from src.shared.constants import COLOR_SCHEME


class NullReport:
    def __init__(self, data, v, v_prev, v_comp, v_comp_prev, condo, mapped):
        self.df_null = data
        self.v = v
        self.v_prev = v_prev
        self.v_comp = v_comp
        self.v_comp_prev = v_comp_prev
        self.diff = f"{v} - {v_prev}"
        self.diff_comp = f"{v_comp} - {v_comp_prev}"
        self.version_pairs = [
            self.diff,
            self.diff_comp,
        ]
        self.condo = condo
        self.mapped = mapped

    def __call__(self):
        st.subheader("Null Graph")
        field_range = st.slider(
            "Select how many fields to display (ranked by number of changes in the current version pair)",
            min_value=1,
            max_value=len(self.fields),
            value=[1, 12],
        )

        df = self.df_null
        df = df.loc[
            (df.condo == self.condo)
            & (df.mapped == self.mapped)
            & (df.pair.isin(self.version_pairs)),
            :,
        ].drop_duplicates()

        df_transformed = df.drop(columns=["condo", "mapped", "total"])
        sorted_df = df_transformed.set_index("pair").T
        sorted_df.sort_values(by=self.diff, ascending=False, inplace=True)
        fields_in_order = sorted_df.index.values.tolist()
        df_transformed = self.sort_and_filter_df(
            df_transformed, field_range, fields_in_order
        )

        diff = self.diff in df_transformed.version.unique()
        diff_comp = self.diff_comp in df_transformed.version.unique()

        if not (diff or diff_comp):
            st.write("Null Graph")
            st.info("There is no change in NULL values across three versions.")
            return
        if diff and not diff_comp:
            self.display_graph(df_transformed, False)
        elif diff_comp and not diff:
            self.display_graph(df_transformed, False)
        else:
            self.display_graph(df_transformed, True)

        st.info(
            """
                The above graph highlights records that formerly had a value and are now NULL, or vice versa.
                The number records going from NULL to not NULL or vice versa should be small for any field.
            """
        )
        st.write(df)

    @property
    def fields(self):
        return [
            "borough",
            "block",
            "lot",
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
            "bldgclass",
            "landuse",
            "easements",
            "ownertype",
            "ownername",
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
            "areasource",
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
            "assessland",
            "assesstot",
            "exempttot",
            "yearbuilt",
            "yearalter1",
            "yearalter2",
            "histdist",
            "landmark",
            "builtfar",
            "residfar",
            "commfar",
            "facilfar",
            "borocode",
            "bbl",
            "condono",
            "tract2010",
            "xcoord",
            "ycoord",
            "longitude",
            "latitude",
            "zonemap",
            "zmcode",
            "sanborn",
            "taxmap",
            "edesignum",
            "appbbl",
            "appdate",
            "plutomapid",
            "version",
            "sanitdistrict",
            "healthcenterdistrict",
            "firm07_flag",
            "pfirm15_flag",
        ]

    def sort_and_filter_df(self, df, range, fields):
        selected = fields[range[0] - 1 : range[1]]
        df = df[["pair"] + selected]
        df_transformed = df.set_index("pair").stack().reset_index()
        df_transformed.rename(
            columns={"pair": "version", "level_1": "field", 0: "change"}, inplace=True
        )
        df_transformed.sort_values(by="change", ascending=False, inplace=True)

        return df_transformed

    def display_graph(self, df_transformed, grouped=True):
        kwargs_dict = {
            "data_frame": df_transformed,
            "y": "change",
            "x": "field",
            "color_discrete_sequence": COLOR_SCHEME,
            "height": 400,
            "width": 850,
            "text_auto": True,
            "title": "Null Graph",
        }
        if grouped:
            kwargs_dict["barmode"] = "group"
            kwargs_dict["color"] = "version"
            fig1 = px.bar(**kwargs_dict)
            fig1.update_layout(legend_title_text="Version")
        else:
            fig1 = px.bar(**kwargs_dict)
        fig1.update_xaxes(title="Field")
        fig1.update_yaxes(title="Change in Null")

        st.plotly_chart(fig1)
