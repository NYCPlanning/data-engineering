import itertools
from functools import cached_property
from pathlib import Path

import numpy as np
import pandas as pd

from . import agg_moe


class AggregatedGeography:
    def __init__(self):
        self.year = 2010

    @cached_property
    def lookup_geo(self):
        # find the current decennial year based on given year
        lookup_geo = pd.read_csv(
            Path(__file__).parent.parent
            / f"data/lookup_geo/{self.year}/lookup_geo.csv",
            dtype="str",
        )
        lookup_geo["geoid_block"] = lookup_geo.county_fips + lookup_geo.ctcb2010
        lookup_geo["geoid_block_group"] = lookup_geo.geoid_block.apply(
            lambda x: x[0:12]
        )
        lookup_geo["geoid_tract"] = lookup_geo.county_fips + lookup_geo.ct2010
        lookup_geo["cd_fp_500"] = lookup_geo.apply(
            lambda row: row["cd"] if int(row["fp_500"]) else np.nan, axis=1
        )
        lookup_geo["cd_fp_100"] = lookup_geo.apply(
            lambda row: row["cd"] if int(row["fp_100"]) else np.nan, axis=1
        )
        lookup_geo["cd_park_access"] = lookup_geo.apply(
            lambda row: row["cd"] if int(row["park_access"]) else np.nan, axis=1
        )
        return lookup_geo

    @staticmethod
    def create_output(df, colname):
        """
        this function will calculate the aggregated e and m
        given colname we would like to aggregate over
        """
        return (
            df[[colname, "e"]]
            .groupby([colname])
            .sum()
            .merge(df[[colname, "m"]].groupby([colname]).agg(agg_moe), on=colname)
            .reset_index()
            .rename(columns={colname: "census_geoid"})
        )

    def tract_to_nta(self, df):
        df = df.merge(
            self.lookup_geo[["geoid_tract", "nta"]].drop_duplicates(),
            how="left",
            right_on="geoid_tract",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "nta")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "NTA"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_group_to_cd_fp500(self, df):
        """
        500 yr flood plain aggregation for block group data (ACS)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_fp_500.isna(), ["geoid_block_group", "cd_fp_500"]
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block_group",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_fp_500")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_fp_500"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_group_to_cd_fp100(self, df):
        """
        100 yr flood plain aggregation for block group data (ACS)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_fp_100.isna(), ["geoid_block_group", "cd_fp_100"]
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block_group",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_fp_100")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_fp_100"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_group_to_cd_park_access(self, df):
        """
        walk-to-park access zone aggregation for block group data (acs)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_park_access.isna(),
                ["geoid_block_group", "cd_park_access"],
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block_group",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_park_access")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_park_access"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_to_cd_fp500(self, df):
        """
        500 yr flood plain aggregation for block data (decennial)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_fp_500.isna(), ["geoid_block", "cd_fp_500"]
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_fp_500")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_fp_500"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_to_cd_fp100(self, df):
        """
        100 yr flood plain aggregation for block data (decennial)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_fp_100.isna(), ["geoid_block", "cd_fp_100"]
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_fp_100")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_fp_100"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_to_cd_park_access(self, df):
        """
        walk-to-park access zone aggregation for block data (decennial)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_park_access.isna(),
                ["geoid_block", "cd_park_access"],
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_park_access")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_park_access"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def tract_to_cd(self, df):
        """
        tract to cd
        """
        df = df.merge(
            self.lookup_geo[["geoid_tract", "cd"]].drop_duplicates(),
            how="left",
            right_on="geoid_tract",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    @cached_property
    def options(self):
        return {
            "decennial": {
                "tract": {"NTA": self.tract_to_nta, "cd": self.tract_to_cd},
                "block": {
                    "cd_fp_500": self.block_to_cd_fp500,
                    "cd_fp_100": self.block_to_cd_fp100,
                    "cd_park_access": self.block_to_cd_park_access,
                },
            },
            "acs": {
                "tract": {"NTA": self.tract_to_nta, "cd": self.tract_to_cd},
                "block group": {
                    "cd_fp_500": self.block_group_to_cd_fp500,
                    "cd_fp_100": self.block_group_to_cd_fp100,
                    "cd_park_access": self.block_group_to_cd_park_access,
                },
            },
        }

    @cached_property
    def aggregated_geography(self) -> list:
        list3d = [[list(k.keys()) for k in i.values()] for i in self.options.values()]
        list2d = itertools.chain.from_iterable(list3d)
        return list(set(itertools.chain.from_iterable(list2d)))

    def format_geoid(self, geoid):
        fips_lookup = {"05": "2", "47": "3", "61": "1", "81": "4", "85": "5"}
        # NTA
        if geoid[:2] in ["MN", "QN", "BX", "BK", "SI"]:
            return geoid
        # Community District (PUMA)
        elif geoid[:2] == "79":
            return geoid[-4:]
        # Census tract
        elif len(geoid) == 11:
            boro = fips_lookup.get(geoid[-8:-6])
            return boro + geoid[-6:]
        # Boro
        elif len(geoid) == 5:
            return fips_lookup.get(geoid[-2:])
        # City
        elif geoid == "3651000":
            return 0

    def format_geotype(self, geotype):
        geotypes = {
            "NTA": "NTA",
            "PUMA": "PUMA",
            "tract": "CT",
            "borough": "Boro",
            "city": "City",
            "block": "CB",
            "block group": "CBG",
        }

        return geotypes.get(geotype) + "2010"
