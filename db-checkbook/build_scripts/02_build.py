import pandas as pd
import geopandas as gpd
import client
import re
import os

"""
merge_cpdb_geoms: this function is essentially the same as in my jupyter notebook:
can someone help me think through how to modify it for using the data 
from digital ocean? I think I'm a little confused as to how exactly this needs to change
"""
def get_file_list():
    return

def merge_cpdb_geoms(): 
    """
    :param path: raw checkbook nyc data
    :type data: pandas df
    :return: cleaned checkbook nyc data
    :rtype: pandas df
    """
    def extract_year(filename):
        match = re.search(r'^\d{4}', filename)
        if match:
            return int(match.group())
        return 0
    
    ## stealing this from pluto-enhancements/digital_ocean_utils
    def get_all_filenames_in_folder():
        filenames = set()
        for filenames in os.listdir(directory_path):
            filename = object.key.split("/")[-1]
            if filename != "":
                filenames.add(object.key.split("/")[-1])
        return filenames

    file_list = sorted(file_list, key=lambda x: int(re.search(r'\d+$',x).group()), reverse=True)

    gdf_list = []
    for f in file_list:
        gdf = gpd.read_file(path + f)
        gdf_list.append(gdf)

    all_cpdb_geoms = pd.concat(gdf_list)
    # note: currently, we're keeping the latest geometry when there are multiple,
    # but we should note that more precise logic for choosing a geometry 
    # should be implemented in the future
    all_cpdb_geoms.drop_duplicates(subset='maprojid', keep='first', inplace=True, ignore_index=True)
    return all_cpdb_geoms

def clean_checkbook(data):
    """
    :param data: raw checkbook nyc data
    :type data: pandas df
    :return: cleaned checkbook nyc data
    :rtype: pandas df
    """
    # TO DO: other data cleaning?
    data = data[data['Check Amount']<99000000] # removing strange seemingly lump sum checks 
    data = data[data['Check Amount']>=0]
    data['FMS ID'] = data['Capital Project'].str.replace(r'\s*d+$','') # QA this output because seems this causes an issue with SCA data
    return data

def group_checkbook(data): 
    """
    :param data: cleaned checkbook nyc data
    :type data: pandas df
    :return: checkbook nyc data grouped by capital project
    :rtype: pandas df
    """
    # is defining a mini-function within the scope of this function fine? in terms of best practices/code organization?  
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

def join_checkbook_geoms(df, cpdb_geoms):
    """
    :param df: Checkbook NYC data collapsed on FMS ID
    :df type: pandas df
    :param cpdb_geoms: final versions of archived CPDB geometries from every year, and the most recent geometry for the current year
    :cpdb_geoms type: geopandas df
    :return: CPDB geometries left-joined onto Checkbook NYC data 
    :rtype: pandas df
    """
    merged = df.merge(cpdb_geoms, how='left', left_on='FMS ID', right_on='maprojid', indicator=True)
    merged = df.drop('Unnamed: 0', axis=1, inplace=True) # TO DO: figure out how to avoid this column appearing in the first place
    gdf = gpd.GeoDataFrame(merged, geometry='geometry')
    return


if __name__ == "__main__":
    print("started build ...")
    cpdb = merge_cpdb_geoms()

    ## TO DO: call functions above
    
