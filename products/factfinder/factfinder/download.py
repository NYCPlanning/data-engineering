import importlib
import os
from functools import cached_property
from pathlib import Path

import numpy as np
import pandas as pd
from census import Census

from .metadata import Metadata, Variable
from .utils import outliers, write_to_cache


class Download:
    def __init__(self, api_key, year=2019, source="acs", geography=2010) -> None:
        self.c = Census(api_key)
        self.year = year
        self.source = source
        self.state = 36
        self.counties = ["005", "081", "085", "047", "061"]
        self.geography = geography
        self.client_options = {
            "D": self.c.acs5dp,
            "S": self.c.acs5st,
            "P": self.c.sf1,
            "B": self.c.acs5,
        }

    @cached_property
    def geoqueries(self):
        return {
            "tract": [
                {"for": "tract:*", "in": f"state:{self.state} county:{county}"}
                for county in self.counties
            ],
            "borough": [
                {"for": f"county:{county}", "in": f"state:{self.state}"}
                for county in self.counties
            ],
            "city": [{"for": "place:51000", "in": f"state:{self.state}"}],
            "block": [
                {"for": "block:*", "in": f"state:{self.state} county:{county}"}
                for county in self.counties
            ],
            "block group": [
                {"for": "block group:*", "in": f"state:{self.state} county:{county}"}
                for county in self.counties
            ],
        }

    def download_variable(
        self, download_function: callable, geotype: dict, v: Variable
    ) -> pd.DataFrame:
        geoqueries = self.geoqueries.get(geotype)
        dfs = []
        for geoquery in geoqueries:
            dfs.append(download_function(geotype, geoquery, v))
        return pd.concat(dfs)

    def download_e_m_p_z(
        self, geotype: str, geoquery: dict, v: Variable
    ) -> pd.DataFrame:
        """
        This function is for downloading non-aggregated-geotype and data profile only
        variables. It will return e, m, p, z variables for a single pff variable.
        """
        # single source (data profile) only, so safe to set a default
        client = self.c.acs5dp
        E, M, PE, PM = v.census_variables
        E_variables, M_variables, PE_variables, PM_variables = E[0], M[0], PE[0], PM[0]
        variables = [E_variables, M_variables, PE_variables, PM_variables]
        df = pd.DataFrame(
            client.get(("NAME", ",".join(variables)), geoquery, year=self.year)
        )
        # If E is an outlier, then set M as Nan
        for var in variables:  # Enforce type safety
            df[var] = df[var].astype("float64")
        df.loc[df[E_variables].isin(outliers), M_variables] = np.nan
        df.loc[df[E_variables] == 0, M_variables] = 0
        # 555555555 indicates controled value,
        # for city and borough, we will set it to 0
        if geotype in ("city", "borough"):
            df.loc[df[M_variables].isin([-555555555, 555555555]), M_variables] = 0
            df.loc[df[PM_variables].isin([-555555555, 555555555]), PM_variables] = 0
        # Replace all outliers as Nan
        df = df.replace(outliers, np.nan)
        return df

    def download_e_m(self, geotype: str, geoquery: dict, v: Variable) -> pd.DataFrame:
        """
        this function works in conjunction with download_variable,
        and is only created to facilitate multiprocessing, this function
        if for generic variable calculation, returns e, m
        """
        # Get unique sources
        sources = set([i[0] for i in v.census_variable])
        frames = []
        for source in sources:
            # Create Variables for given source and set client
            variables = [i for i in v.census_variable if i[0] == source]
            client = self.client_options.get(source, self.c.acs5)
            # create_census_variables Will be deprecated eventually
            E_variables, M_variables = v.create_census_variables(variables)
            frames.append(
                pd.DataFrame(
                    client.get(
                        ("NAME", ",".join(E_variables + M_variables)),
                        geoquery,
                        year=self.year,
                    )
                )
            )
        # Combine results from each source by joining on geo name
        df = frames[0]
        for i in frames[1:]:
            df = pd.merge(
                df,
                i[i.columns.difference(["state", "county", "tract", "place"])],
                left_on="NAME",
                right_on="NAME",
            )
        del frames
        # Enforce type safety
        for i in v.census_variable:
            if i[0] != "P":
                df[f"{i}E"] = df[f"{i}E"].astype("float64")
                df[f"{i}M"] = df[f"{i}M"].astype("float64")
                # If E is zero, then set M as zero
                df.loc[df[f"{i}E"] == 0, f"{i}M"] = 0
                # If E is an outlier, then set M as Nan
                df.loc[df[f"{i}E"].isin(outliers), f"{i}M"] = np.nan
            else:
                df[i] = df[i].astype("float64")
            # 555555555 indicates controled value,
            # for city and borough, we will set it to 0
            if geotype in ("city", "borough"):
                df.loc[df[f"{i}M"].isin([-555555555, 555555555]), f"{i}M"] = 0

        # Replace all outliers as Nan
        df = df.replace(outliers, np.nan)
        return df

    def create_census_geoid(self, df: pd.DataFrame, geotype: str) -> pd.DataFrame:
        if geotype == "tract":
            df["census_geoid"] = df["state"] + df["county"] + df["tract"]
        elif geotype == "borough":
            df["census_geoid"] = df["state"] + df["county"]
        elif geotype == "city":
            df["census_geoid"] = df["state"] + df["place"]
        elif geotype == "block":
            df["census_geoid"] = df["state"] + df["county"] + df["tract"] + df["block"]
        elif geotype == "block group":
            df["census_geoid"] = (
                df["state"] + df["county"] + df["tract"] + df["block group"]
            )
        return df

    def __call__(self, geotype: str, pff_variable: str) -> pd.DataFrame:
        cache_path = (
            ".cache/download"
            f"/year={self.year}"
            f"/geography={self.geography}"
            f"/geotype={geotype}"
            f"/{pff_variable}.pkl"
        )
        if os.path.isfile(cache_path):
            df = pd.read_pickle(cache_path)
        else:
            meta = Metadata(year=self.year, source=self.source)
            AggregatedGeography = importlib.import_module(
                f"factfinder.geography.{self.geography}"
            ).AggregatedGeography
            geography = AggregatedGeography()

            v = meta.create_variable(pff_variable)
            if (
                pff_variable in meta.profile_only_variables
                and geotype not in geography.aggregated_geography
            ):
                # For profile only variables we will get e, m, p, z
                df = self.download_variable(self.download_e_m_p_z, geotype, v)
            else:
                df = self.download_variable(self.download_e_m, geotype, v)
            df = self.create_census_geoid(df, geotype)
            df["geotype"] = geotype
            df["pff_variable"] = pff_variable
            os.makedirs(Path(cache_path).parent, exist_ok=True)
            write_to_cache(df, cache_path)
        return df
