# This is the beginning of a QA script to compare previous and current versions of EDDE.
# There's not a fixed use-case for it - there's not really an EDDE QA process to speak of
# since other teams assume EDDE to be a pretty straight inputs -> outputs build map.

# Leaving this here as something for the next data engineer to potentially pick up.


from aggregate import load_aggregated as load_agg
from pathlib import Path
import pandas as pd

# For comparisons to older versions of EDDE, we'd need to map old PUMAS to
# new. I just used Park Slope, but it might be nice to fill this out.
PARK_SLOPE_PUMA_OLD = "04005"
PARK_SLOPE_PUMA_NEW = "04306"
puma_2010_2020_known_equiv = {PARK_SLOPE_PUMA_OLD: PARK_SLOPE_PUMA_NEW}

acs = load_agg.load_acs("0812")

new_build_path = Path("FILL ME IN")
old_build_path = Path("FILL ME IN")


geos = ["citywide", "borough", "puma"]
categories = [
    "demographics",
    "housing_security",
    "economics",
    "quality_of_life",
    "housing_production",
]


def all_indicators(build_path, version):
    """Read in all indicator files into a single dataframe from a single build"""
    combined_df = None
    for category in categories:
        print(category)
        path = Path(build_path / category)
        assert path.exists()

        for file in [f for f in path.iterdir() if f.name != "metadata.yml"]:
            ind_csv_parts = (
                file.name.replace(f"{category}_", "").replace(".csv", "").split("_")
            )
            year = geo = None
            if len(ind_csv_parts) == 2:
                year, geo = ind_csv_parts
            else:
                geo = ind_csv_parts[0]

            df = pd.read_csv(file, dtype={geo: str})
            df = df.melt(geo).rename(columns={geo: "geo"})
            df["geo2"] = df.geo
            df["category"] = category
            df["geo_type"] = geo
            df["version"] = version
            df["census_year"] = year or ""
            df["full_variable"] = df["variable"] + df["census_year"]
            if combined_df is not None:
                combined_df = pd.concat([combined_df, df])
            else:
                combined_df = df
    return combined_df


new_ind = all_indicators(new_build_path, "current")
old_ind = all_indicators(old_build_path, "previous")

all_together = pd.concat([new_ind, old_ind])

indexed = all_together.set_index(
    ["geo_type", "category", "full_variable", "geo", "version"]
).sort_index()
