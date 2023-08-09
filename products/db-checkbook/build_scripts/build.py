import pandas as pd
import geopandas as gpd
import re
from pathlib import Path
from sqlalchemy import create_engine, text

from . import LIB_DIR, OUTPUT_DIR, SQL_QUERY_DIR

DB_URL = "sqlite:///checkbook.db"
ENGINE = create_engine(DB_URL)


def _read_all_cpdb_geoms(dir=LIB_DIR) -> list:
    """
    :return: list of gdfs, one for each cpdb folder
    """

    def extract_year(filename):
        # TODO: error checking
        match = re.search(r"^\d{4}", filename)
        if match:
            return int(match.group())
        return None

    subdir_list = [p.name for p in dir.iterdir() if p.is_dir()]
    subdir_list = sorted(
        subdir_list, key=lambda x: extract_year(x), reverse=True
    )  # sort by year
    gdf_list = []

    for f in subdir_list:
        gdf = gpd.read_file(dir / f)
        gdf_list.append(gdf)

    return gdf_list


def _merge_cpdb_geoms(gdf_list: list[str]) -> gpd.GeoDataFrame:
    """
    :return: merged cpdb geometries
    """
    if not gdf_list:
        gdf_list = _read_all_cpdb_geoms()

    all_cpdb_geoms = pd.concat(gdf_list)
    # NOTE: keeping the latest geometry when there are multiple
    all_cpdb_geoms.drop_duplicates(
        subset="maprojid", keep="first", inplace=True, ignore_index=True
    )
    return all_cpdb_geoms


def _read_checkbook(dir: Path = LIB_DIR, f: str = "nycoc_checkbook.csv"):
    """
    :return: raw checkbook data as pd df
    """
    df = pd.read_csv(dir / f)
    return df


def _clean_checkbook(df: pd.DataFrame) -> pd.DataFrame:
    """
    :return: cleaned checkbook nyc data
    """
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.lower()
    # NOTE: This data cleaning is NOT complete, and we should investigate other cases where we should omit data
    df = df[df["check_amount"] < 99000000]
    df = df[df["check_amount"] >= 0]
    # taking out white space from capital project for join with cpdb
    """
    remove last three digits and any trailing whitespace from `Capital Project`
    Example: `Capital Project` = '998CAP2024  005' -> `FMS ID` = '998CAP2024'
    \s*: matches zero or more whitespace characters
    d+: matches one or more digits
    $: assert position at the end of the string
    """
    df["fms_id"] = df["capital_project"].str.replace(
        r"\s*\d+$", ""
    )  # QA this output because seems this causes an issue with SCA data
    return df


def _group_checkbook(data: pd.DataFrame) -> pd.DataFrame:
    """
    :return: checkbook nyc data grouped by capital project
    """
    def fn_join_vals(x):
        return ";".join([y for y in list(x) if pd.notna(y)])

    cols_for_grouping = ["fms_id"]
    cols_for_limiting = cols_for_grouping + [
        "contract_purpose",
        "agency",
        "budget_code",
        "check_amount",
    ]
    df_limited_cols = data.loc[:, cols_for_limiting]
    agg_dict = {
        "check_amount": "sum",
        "contract_purpose": fn_join_vals,
        "budget_code": fn_join_vals,
        "agency": fn_join_vals,
    }
    df = df_limited_cols.groupby(cols_for_grouping, as_index=False).agg(agg_dict)
    return df


def _join_checkbook_geoms(
    df: pd.DataFrame, cpdb_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    :param df: Checkbook NYC data collapsed on FMS ID
    :param cpdb_geoms: final versions of archived CPDB geometries from every year, and the most recent geometry for the current year
    :return: CPDB geometries left-joined onto Checkbook NYC data
    """
    merged = df.merge(
        cpdb_gdf, how="left", left_on="fms_id", right_on="maprojid", indicator=True
    )
    gdf = gpd.GeoDataFrame(merged, geometry="geometry")
    return gdf


def _assign_checkbook_category(df: pd.DataFrame, sql_dir=SQL_QUERY_DIR) -> pd.DataFrame:
    """
    param df: cleaned and collapsed checkbook NYC data
    return: pandas df of checkbook data with category assignment based on specified col
    """
    target_cols = {"budget_code": "bc_category", "contract_purpose": "cp_category"}
    df["bc_category"] = None
    df["cp_category"] = None

    with ENGINE.connect() as conn:
        df.to_sql("capital_projects", ENGINE, if_exists="replace", index=False)
        for k, v in target_cols.items():
            with open(sql_dir / "categorization.sql", "r") as query_file:
                query = query_file.read()
            query = query.replace("COLUMN", k)
            query = query.replace("col_category", v)
            queries = [q for q in query.split(";")]
            for q in queries:
                conn.execute(text(q))

        ret = pd.read_sql_table("capital_projects", conn)

    return ret


def _clean_joined_checkbook_cpdb(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    :param gdf: joined cpdb and checkbook nyc data
    :return: cleaned joined checkbook cpdb data
    """
    gdf.rename(columns={"typecatego": "cpdb_category"}, inplace=True)
    gdf["has_geometry"] = gdf["_merge"].map(lambda x: x == "both")
    gdf["cpdb_category"].fillna("None", inplace=True)
    gdf["bc_category"].fillna("None", inplace=True)
    gdf["cp_category"].fillna("None", inplace=True)
    gdf.drop("_merge", axis=1, inplace=True)
    return _limit_cols(gdf)


def _limit_cols(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    :param gdf: joined checkbook nyc and cpdb data
    :return: geopandas df of joined checkbook cpdb data with cols limited/reordered
    """
    cols = [
        "fms_id",
        "check_amount",
        "contract_purpose",
        "budget_code",
        "agency",
        "bc_category",
        "cp_category",
        "cpdb_category",
        "ccpversion",
        "maprojid",
        "magency",
        "magencyacr",
        "projectid",
        "descriptio",
        "geomsource",
        "dataname",
        "datasource",
        "datadate",
        "geometry",
        "cartodb_id",
        "has_geometry",
    ]

    filtered_cols = [col for col in cols if col in gdf.columns]
    return gdf[filtered_cols]


def _assign_final_category(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    return: geopandas gdf with merged checkbook cpdb data and
    final category assignment using high sensitivity fixed asset method
    """
    cols = ["cpdb_category", "bc_category", "cp_category"]
    cats = ["Fixed Asset", "ITT, Vehicles, and Equipment", "Lump Sum", "None"]

    def assign_category(row):
        for cat in cats[:3]:
            if cat in row.values:
                return cat
        return cats[-1]

    gdf["final_category"] = gdf[cols].apply(lambda row: assign_category(row), axis=1)
    return gdf


def run_build() -> None:
    """
    :return: historical spending data
    """
    print("read in source data...")
    raw_checkbook = _read_checkbook()
    cpdb_list = _read_all_cpdb_geoms()
    print("_clean_checkbook...")
    clean_checkbook = _clean_checkbook(raw_checkbook)
    print("merge and group source data ...")
    cpdb_geoms = _merge_cpdb_geoms(cpdb_list)
    grouped_checkbook = _group_checkbook(clean_checkbook)
    print("_assign_checkbook_category ...")
    cat_checkbook = _assign_checkbook_category(grouped_checkbook)
    print("_join_checkbook_geoms ...")
    joined_data = _join_checkbook_geoms(cat_checkbook, cpdb_geoms)
    print("_clean_joined_checkbook_cpdb ...")
    cleaned_data = _clean_joined_checkbook_cpdb(joined_data)
    print("_assign_final_category ...")
    final_data = _assign_final_category(cleaned_data)
    print("save temp csv ...")
    fp = OUTPUT_DIR / "historical_spend.csv"
    final_data.to_csv(fp)


if __name__ == "__main__":
    print("started build ...")
    run_build()
    print("done!")
