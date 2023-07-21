from dcpy.connectors.s3 import client
import re
import fuzzymatcher


# regex patterns

street_mentions_regex_pattern = '(\d+\s+AVE|\d+\s+STREET|BLOCK|LOT|\d+\s+ST|\d+\s+AVENUE)+'

# keywords

list_facilities = [
    'Canal', 'Facility', 'Garage', 'Garden', 'Greenway', 'Hall', 'Hospital', 'Library', 
    'Museum', 'Pier', 'Plant', 'Playground', 'Plaza', 'Room', 'Sewer', 'Shelter', 'Stadium', 
    'Station', 'Street', 'Toilet', 'Tunnel', 'Waste', 'Bikeway', 'Campus', 'College', 'Demolition', 
    'Entrance', 'Expansion', 'Extension', 'Facade', 'Installation', 'Marina', 
    'Overpass', 'Underpass', 'Refurb', 'Relocat', 'Removal', 'Stair', 
    'Tower', 'Warehouse', 'Water Tower', 'Zoo', 'YMCA', 'YWHA'
]

# utils function: fuzzy matching on city facilities

def matcher_check_facility(col_name, checkbook_df, city_facilities_df):
    col_name = col_name.upper()
    regex_pattern_check = '[A-Z]+\s+'+col_name
    pattern_results = checkbook_df['Budget Code'].apply(lambda x: re.findall(regex_pattern_check, str(x))[0] if re.findall(regex_pattern_check, str(x)) != [] else None)
    checkbook_df[col_name] = pattern_results 
    facility_mentions = city_facilities_df[city_facilities_df['PARCELNAME'].str.contains(col_name) == True]
    facilities = checkbook_df[checkbook_df['Budget Code'].str.contains(col_name) == True]
    fuzzy_fac = fuzzymatcher.fuzzy_left_join(facilities, facility_mentions, left_on = col_name, right_on = "PARCELNAME")
    return fuzzy_fac[fuzzy_fac['best_match_score'] > 0][['Budget Code', 'PARCELNAME', 'best_match_score', col_name]]

# utils: percentage keyword calculation

def percentage_keywordS(df, list_fac):
    percentages_dict = {}
    percentages_money = {}
    for object in list_fac:
        col_name = object.upper()
        regex_pattern_check = '[A-Z]+\s+'+ col_name
        pattern_results = df['Budget Code'].apply(lambda x: re.findall(regex_pattern_check, str(x))[0] if re.findall(regex_pattern_check, str(x)) != [] else None)
        df[col_name] = pattern_results 
        percent_col = df[df[col_name].str.contains(col_name) == True].shape[0]/df.shape[0]
        perc_mon = sum(df[df[col_name].str.contains(col_name) == True]['Check Amount'])
        percentages_dict[object] = percent_col
        percentages_money[object] = perc_mon
    return percentages_dict, percentages_money

# visualization functions


if __name__ == "__main__":
    print("started export ...")


