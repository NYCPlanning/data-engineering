import pandas as pd
import geopandas as gpd
import re
import os
from pathlib import Path
import sqlalchemy
from sqlalchemy import create_engine, text

_curr_file_path = Path(__file__).resolve()
LIB_DIR = _curr_file_path.parent.parent / '.library'
SQL_QUERY_DIR = _curr_file_path.parent.parent / 'sql_query'

# should these be declared as constants, or in a function?
DB_URL = 'sqlite:///checkbook.db'
ENGINE = create_engine(DB_URL)

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
    
    subdir_list = [p.name for p in LIB_DIR.iterdir() if p.is_dir()]
    subdir_list = sorted(subdir_list, key=lambda x: extract_year(x), reverse=True) # sort by year
    gdf_list = []
    
    for f in subdir_list:
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
    data = pd.read_csv(LIB_DIR / 'nycoc_checkbook.csv')
    # NOTE: This data cleaning is NOT complete, and we should investigate other cases where we should omit data
    data = data[data['check_amount']<99000000]
    data = data[data['check_amount']>=0]
    # taking out white space from capital project for join with cpdb 
    """
    remove last three digits and any trailing whitespace from `Capital Project`
    Example: `Capital Project` = '998CAP2024  005' -> `FMS ID` = '998CAP2024'
    \s*: matches zero or more whitespace characters
    d+: matches one or more digits
    $: assert position at the end of the string
    """
    data['fms_id'] = data['capital_project'].str.replace(r'\s*d+$','') # QA this output because seems this causes an issue with SCA data
    return data

def _group_checkbook(data: pd.DataFrame) -> pd.DataFrame: 
    """
    :param data: cleaned checkbook nyc data
    :return: checkbook nyc data grouped by capital project
    """
    def fn_join_vals(x):
        return ';'.join([y for y in list(x) if pd.notna(y)])

    cols_for_grouping = ['fms_id']
    cols_for_limiting = cols_for_grouping + [
        'contract_purpose', 
        'agency', 
        'budget_code', 
        'check_amount'
    ]
    df_limited_cols = data.loc[:, cols_for_limiting]
    agg_dict = {
        'check_amount': 'sum',
        'contract_purpose': fn_join_vals,
        'budget_code': fn_join_vals,
        'agency': fn_join_vals
    }

    df = df_limited_cols.groupby(cols_for_grouping, as_index=False).agg(agg_dict)
    return df

def _join_checkbook_geoms(df: pd.DataFrame, cpdb_geoms: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    :param df: Checkbook NYC data collapsed on FMS ID
    :param cpdb_geoms: final versions of archived CPDB geometries from every year, and the most recent geometry for the current year
    :return: CPDB geometries left-joined onto Checkbook NYC data 
    """
    merged = df.merge(cpdb_geoms, how='left', left_on='fms_id', right_on='maprojid', indicator=True)
    #merged = df.drop('Unnamed: 0', axis=1, inplace=True) # TODO: figure out how to avoid this column appearing in the first place!
    gdf = gpd.GeoDataFrame(merged, geometry='geometry')
    return gdf

def _df_to_sql(df: pd.DataFrame) -> None:
    table_name = 'capital_projects'
    df.to_sql(table_name, ENGINE, if_exists='replace', index=False)
    return

def _assign_checkbook_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    param df: cleaned and collapsed checkbook NYC data 
    return: pandas df of checkbook data with category assignment based on specified col 
    """
    queries = [SQL_QUERY_DIR / 'query_itt_vehicles_equipment.sql', SQL_QUERY_DIR / 'query_lump_sum.sql', SQL_QUERY_DIR / 'query_fixed_asset.sql']
    target_cols = {'budget_code': 'bc_category', 'contract_purpose': 'cp_category'}
    df['bc_category'] = None
    df['cp_category'] = None
    
    _df_to_sql(df)

    with ENGINE.connect() as conn:
        for k, v in target_cols.items():
            with open(SQL_QUERY_DIR / 'query.sql', 'r') as query_file:
                query = query_file.read()
            query = query.replace('COLUMN', k)
            query = query.replace('col_category', v)
            queries = [q.strip() for q in query.split(';') if q.strip()]
            for q in queries:
                conn.execute(text(q))
    
        ret = pd.read_sql_table('capital_projects', conn)

    return ret

def _assign_final_category(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    return: geopandas gdf with merged checkbook cpdb data and 
    final category assignment using high sensitivity fixed asset method
    """ 
    return

if __name__ == "__main__":
    print("started build ...")
    #cpdb_geoms = _merge_cpdb_geoms()
    #print('Merged CPDB geoms')
    cleaned_checkbook = _clean_checkbook() 
    print('Cleaned checkbook data')
    grouped_checkbook = _group_checkbook(cleaned_checkbook)
    print('grouped checkbook data')
    #joined_data = _join_checkbook_geoms(grouped_checkbook, cpdb_geoms)
    #print('joined checkbook and cpdb data')

    cat_checkbook = _assign_checkbook_category(grouped_checkbook)
    print('assigned cats')
    print(cat_checkbook.head(5))
    # TODO: call categorization functions
    # TODO: save outputs
