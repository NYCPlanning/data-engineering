import pandas as pd
from dotenv import load_dotenv
from src.digital_ocean_utils import DigitalOceanClient

load_dotenv()

cpdb_published_versions = [
    "2022-10-25",  # '22 CPDB Adopted
    "2022-06-08",  # '22 CPDB Executive
    "2022-04-15",  # '22 CPDB Prelminary
]


BUCKET_NAME = "edm-publishing"
REPO_NAME = "db-cpdb"

VIZKEY = {
    "all categories": {
        "projects": {"values": ["totalcount", "mapped"]},
        "commitments": {"values": ["totalcommit", "mappedcommit"]},
    },
    "fixed assets": {
        "projects": {"values": ["fixedasset", "fixedassetmapped"]},
        "commitments": {"values": ["fixedassetcommit", "fixedassetmappedcommit"]},
    },
}

"""a feedback from the group is to have a dictionary from the abbreviation for agency for something more explicity
where would this list comes from? """


def get_geometries(branch, table) -> dict:
    client = DigitalOceanClient(bucket_name=BUCKET_NAME, repo_name=REPO_NAME)

    gdf = client.shapefile_from_DO(
        shapefile_zip=f"db-cpdb/{branch}/latest/output/{table}.shp.zip"
    )

    return gdf


def get_data(branch, previous_branch, previous_version) -> dict:
    rv = {}
    tables = {
        "analysis": ["cpdb_summarystats_sagency", "cpdb_summarystats_magency"],
        "others": ["cpdb_adminbounds"],
        "no_version_compare": ["geospatial_check"],
        "geometries": ["cpdb_dcpattributes_pts", "cpdb_dcpattributes_poly"],
    }

    client = DigitalOceanClient(bucket_name=BUCKET_NAME, repo_name=REPO_NAME)

    for t in tables["analysis"]:
        rv[t] = client.csv_from_DO(url=construct_url(branch, t, sub_folder="analysis/"))
        rv["pre_" + t] = client.csv_from_DO(
            url=construct_url(
                previous_branch, t, previous_version, sub_folder="analysis/"
            )
        )
    for t in tables["others"]:
        rv[t] = client.csv_from_DO(url=construct_url(branch, t))
        rv["pre_" + t] = client.csv_from_DO(
            url=construct_url(previous_branch, t, previous_version)
        )
    for t in tables["no_version_compare"]:
        rv[t] = client.csv_from_DO(url=construct_url(branch, t))
    for t in tables["geometries"]:
        rv[t] = get_geometries(branch, table=t)
    return rv


def get_commit_cols(df: pd.DataFrame):
    full_cols = df.columns
    cols = [c for c in full_cols if "commit" in c]
    return cols


def get_diff_dataframe(df: pd.DataFrame, df_pre: pd.DataFrame):
    diff = df.sub(df_pre, fill_value=0)
    return diff


def get_map_percent_diff(df: pd.DataFrame, df_pre: pd.DataFrame, keys: dict):
    diff = pd.DataFrame(
        columns=["percent_mapped", "pre_percent_mapped", "diff_percent_mapped"],
        index=df.index,
    )

    diff["percent_mapped"] = df[keys["values"][1]] / df[keys["values"][0]]
    diff["pre_percent_mapped"] = df_pre[keys["values"][1]] / df_pre[keys["values"][0]]
    diff["diff_percent_mapped"] = diff.percent_mapped - diff.pre_percent_mapped
    diff.sort_values(by="diff_percent_mapped", inplace=True, ascending=True)

    return diff


def sort_base_on_option(
    df: pd.DataFrame, subcategory, view_type, map_option, ascending=True
):
    df_sort = df.sort_values(
        by=VIZKEY[subcategory][view_type]["values"][map_option], ascending=ascending
    )

    return df_sort


def construct_url(branch, table, build="latest", sub_folder=""):
    return (
        f"https://edm-publishing.nyc3.digitaloceanspaces.com/db-cpdb/{branch}"
        f"/{build}/output/{sub_folder}{table}.csv"
    )
