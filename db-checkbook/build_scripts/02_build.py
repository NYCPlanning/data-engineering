import pandas as pd
import geopandas as gpd
import re
import os
from pathlib import Path

_curr_file_path = Path(__file__).resolve()
LIB_DIR = _curr_file_path / '..' / '.library'

def _merge_cpdb_geoms() -> gpd.GeoDataFrame: 
    """
    :return: cleaned checkbook nyc data
    :rtype: pandas df
    """
    def extract_year(filename):
        # TODO: error checking
        match = re.search(r'^\d{4}', filename)
        if match:
            return int(match.group())
        return None
    
    file_list = [p.name for p in LIB_DIR.iterdir() if p.is_file()]
    file_list = sorted(file_list, key=lambda x: extract_year(x), reverse=True) # sort by year

    gdf_list = []
    for f in file_list:
        gdf = gpd.read_file(LIB_DIR / f)
        gdf_list.append(gdf)

    all_cpdb_geoms = pd.concat(gdf_list)
    # NOTE: keeping the latest geometry when there are multiple
    all_cpdb_geoms.drop_duplicates(subset='maprojid', keep='first', inplace=True, ignore_index=True)
    return all_cpdb_geoms

def _clean_checkbook() -> pd.DataFrame:
    """
    :return: cleaned checkbook nyc data
    :rtype: pandas df
    """
    data = pd.read_csv(LIB_DIR + 'nycoc_checkbook.csv')
    # NOTE: This data cleaning is NOT complete, and we should investigate other cases where we should omit data
    data = data[data['Check Amount']<99000000]
    data = data[data['Check Amount']>=0]
    # taking out white space from capital project for join with cpdb 
    """
    remove last three digits and any trailing whitespace from `Capital Project`
    Example: `Capital Project` = '998CAP2024  005' -> `FMS ID` = '998CAP2024'
    \s*: matches zero or more whitespace characters
    d+: matches one or more digits
    $: assert position at the end of the string
    """
    data['FMS ID'] = data['Capital Project'].str.replace(r'\s*d+$','') # QA this output because seems this causes an issue with SCA data
    return data

def _group_checkbook(data: pd.DataFrame) -> pd.DataFrame: 
    """
    :param data: cleaned checkbook nyc data
    :return: checkbook nyc data grouped by capital project
    """
    def fn_join_vals(x):
        return ';'.join([y for y in list(x) if pd.notna(y)])

    cols_for_grouping = ['FMS ID']
    cols_for_limiting = cols_for_grouping + [
        'Contract Purpose', 
        'Agency', 
        'Budget Code', 
        'Check Amount'
    ]
    df_limited_cols = data.loc[:, cols_for_limiting]
    agg_dict = {
        'Check Amount': 'sum',
        'Contract Purpose': fn_join_vals,
        'Budget Code': fn_join_vals,
        'Agency': fn_join_vals
    }

    df = df_limited_cols.groupby(cols_for_grouping, as_index=False).agg(agg_dict)
    return df

def _join_checkbook_geoms(df: pd.DataFrame, cpdb_geoms: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    :param df: Checkbook NYC data collapsed on FMS ID
    :param cpdb_geoms: final versions of archived CPDB geometries from every year, and the most recent geometry for the current year
    :return: CPDB geometries left-joined onto Checkbook NYC data 
    """
    merged = df.merge(cpdb_geoms, how='left', left_on='FMS ID', right_on='maprojid', indicator=True)
    merged = df.drop('Unnamed: 0', axis=1, inplace=True) # TODO: figure out how to avoid this column appearing in the first place!
    gdf = gpd.GeoDataFrame(merged, geometry='geometry')
    return gdf

# ----  TODO: category assignment on BC, CP, and high-sensitivity Fixed Asset approach ----

def _

if __name__ == "__main__":

    print("started build ...")
    cpdb_geoms = _merge_cpdb_geoms()
    cleaned_checkbook = _clean_checkbook() 
    grouped_checkbook = _group_checkbook(cleaned_checkbook)
    joined_data = _join_checkbook_geoms(grouped_checkbook, cpdb_geoms)
    # TODO: call categorization functions
    # TODO: save outputs
