import pandas as pd
import json
import simplejson
import numpy as np

###
DB_EDDT_TARGET= "2025-05-30"
assert DB_EDDT_TARGET != "", "DB_EDDT_TARGET must point valid db ebbt DO Space folder"

###
OUTPUT_FOLDER = 'output'

category_folders = {
  "demo": "demographics",
  "econ": "economics",
  "hsaq": "housing_security",
  "hopd": "housing_production",
  "qlao": "quality_of_life"
}

DO_EDM_URL = f'https://edm-publishing.nyc3.digitaloceanspaces.com/db-eddt/publish/{DB_EDDT_TARGET}'
DO_CHANGE_URL = "https://equity-tool-data.nyc3.digitaloceanspaces.com/change/latest"

geography_column_names = {
  "district": "puma",
  "borough": "borough",
  "citywide": "citywide"
}

borough_map = {
      "MN": "1",
      "BX": "2",
      "BK": "3",
      "QN": "4",
      "SI": "5"
    }

subgroup_tokens = {
  "tot": "",
  "anh": "_anh",
  "bnh": "_bnh",
  "wnh": "_wnh",
  "hsp": "_hsp"
}

variances = {
  "NONE": {
    "token": ""
  },
  "MOE": {
    "token": "_moe"
  },
  "CV": {
    "token": "_cv"
  }
}

measures = {
  "COUNT": {
    "token": "_count"
  },
  "PERCENT": {
    "token": "_pct"
  },
  "RATE": {
    "token": "_rate"
  },
  "INDEX": {
    "token": "_index"
  },
  "MEDIAN": {
    "token": "_median"
  },
  "PERCENTAGE_POINT": {
    "token": "_pnt"
  }

}


###
# FUNCTIONS
###

def load_json(path):
  file = open(path, 'r')
  return json.load(file)


# Recursively deep merges two dictionaries and returns the result
# https://stackoverflow.com/a/20666342
def merge(source, destination):
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            merge(value, node)
        else:
            destination[key] = value

    return destination


# Downloads EDM csv file for given geography, category, and filename
# Sets column containing geoids as index, and adds column axis multi-index with value of filename
def download_df(geography, category, filename, appended_columns, copy_columns):
  base_url = DO_CHANGE_URL if "change" in filename else DO_EDM_URL
  dest = f'{base_url}/{category_folders[category]}/{filename}.csv'
  df = pd.read_csv(dest, dtype={geography_column_names[geography]: "str"})
  if filename in appended_columns:
    for column_name, value in appended_columns[filename]:
      df[column_name] = value
  if filename in copy_columns:
    for source, dest in copy_columns[filename].items():
      df[dest] = df[source]
  if geography == "district":
    df[geography_column_names[geography]] = df[geography_column_names[geography]].str.lstrip('0')
  try:
    df = df.set_index(geography_column_names[geography])
  except KeyError:
    raise KeyError(f'Unable to set index for file {filename}, category {category}, and geography {geography}')

  df.columns = pd.MultiIndex.from_tuples([(filename, col_name) for col_name in df.columns.tolist()], names=["file","column"])
  return df


def build_column_name(variable, subgroup, measure, variance, year="", subcategory="", suffix=""):
  measure_token = measures[measure]["token"]
  variance_token = variances[variance]["token"]
  return f'{subcategory}{variable}{suffix}{year}{subgroup_tokens[subgroup]}{measure_token}{variance_token}'


def build_row_column_names(variable, subgroup, measures, variances, year, subcategory, suffixes):
  result = []
  _suffixes = ["" for i in variances] if suffixes == None else suffixes
  for measure, variance, suffix in tuple(zip(measures, variances, _suffixes)):
    result.append(build_column_name(variable, subgroup, measure, variance, year, subcategory, suffix))
  return result


# The values in the "measures" list for change over time vintages will be different than the list
# that describes the non-change vintages but we can figure out what it will be based on the list for
# the non-change vintages, this saves us from having to define them manually for every indicator with a change vintage
def get_change_measures(measures):
  if measures == ["COUNT", "COUNT", "COUNT", "PERCENT", "PERCENT"]:
    return ["COUNT", "COUNT", "PERCENT","PERCENT", "PERCENTAGE_POINT", "PERCENTAGE_POINT"]
  elif measures == ["MEDIAN", "MEDIAN", "MEDIAN", "PERCENT", "PERCENT"]:
    return ["MEDIAN", "MEDIAN", "PERCENT","PERCENT", "PERCENTAGE_POINT", "PERCENTAGE_POINT"]
  elif measures == ["COUNT", "PERCENT"]:
    return ["COUNT", "PERCENT", "PERCENTAGE_POINT"]
  elif measures == ["COUNT", "COUNT", "COUNT"]:
    return ["COUNT","COUNT","PERCENT","PERCENT"]
  elif measures == ["MEDIAN", "MEDIAN", "MEDIAN"]:
    return ["MEDIAN", "MEDIAN", "PERCENT", "PERCENT"]
  elif measures == ["RATE"]:
    return ["RATE", "PERCENT"]
  elif measures == ["COUNT"]:
    return ["COUNT", "PERCENT"]
  else:
    print(f'could not find value for measures {measures}')
    return []


# This function does the same thing as get_change_measures but for variances
def get_change_variances(variances):
  if variances == ["NONE", "MOE", "CV", "NONE", "MOE"]:
    return ["NONE", "MOE","NONE", "MOE","NONE", "MOE"]
  elif variances == ["NONE", "NONE"]:
    return ["NONE", "NONE", "NONE"]
  elif variances == ["NONE", "MOE", "CV"]:
    return ["NONE", "MOE", "NONE", "MOE"]
  elif variances == ["NONE"]:
    return ["NONE", "NONE"]
  else:
    print(f'could not find value for variances {variances}')
    return []


# In most cases, we can derive what the value of "headers" should be based on
# "measures". This function handles that, allowing us to remove the repetitive
# header values from the config files
def get_headers(measures):
  if measures == ["COUNT", "COUNT", "COUNT", "PERCENT", "PERCENT"]:
    return [[["number", 3],["percent", 2]],[["estimate", 1],["moe", 1],["cv", 1],["estimate", 1],["moe", 1]]]
  elif measures == ["COUNT", "PERCENT"]:
    return [[["number", 1],["percent", 1]]]
  elif measures == ["COUNT", "COUNT", "COUNT"]:
    return [[["number", 3]],[["estimate", 1],["moe", 1],["cv", 1]]]
  elif measures == ["MEDIAN", "MEDIAN", "MEDIAN"]:
    return [[["number", 3]],[["estimate", 1],["moe", 1],["cv", 1]]]
  elif measures == ["RATE"]:
    return [[["number", 1]]]
  elif measures == ["COUNT"]:
    return [[["number", 1]]],
  elif measures == ["INDEX"]:
    return [[["score 1-5", 1]]]
  else:
    print(f'could not find headers value for measures {measures}')
    return []


# This function does the same thing as get_headers but for headers of
# change over time vintages
def get_change_headers(measures):
  if measures == ["COUNT", "COUNT", "COUNT", "PERCENT", "PERCENT"]:
    return [[["number", 2],["percent", 2],["pctg. pt.", 2]],[["estimate", 1],["moe", 1],["estimate", 1],["moe", 1],["estimate", 1],["moe", 1]]]
  elif measures == ["COUNT", "PERCENT"]:
    return [[["number", 1],["percent", 1],["pctg. pt.", 1]]]
  elif measures == ["COUNT", "COUNT", "COUNT"]:
    return [[["number", 2],["percent", 2]],[["estimate", 1],["moe", 1],["estimate", 1],["moe", 1]]]
  elif measures == ["MEDIAN", "MEDIAN", "MEDIAN"]:
    return [[["number", 2],["percent", 2]],[["estimate", 1],["moe", 1],["estimate", 1],["moe", 1]]]
  elif measures == ["RATE"]:
    return [["number", 1],["percent", 1]],
  elif measures == ["COUNT"]:
    return [["number", 1],["percent", 1]],
  else:
    print(f'could not find change headers value for measures {measures}')
    return []


def build_config(table_config, subgroup):
  result = {}

  # Copy properties of table_config that should be mapped directly into
  # the built config
  result["title"] = table_config["title"]
  result["id"] = table_config.get("id", None)
  result["vintages"] = table_config["vintages"]
  result["files"] = table_config["files"]
  result["labels"] = table_config["labels"]
  result["variances"] = [table_config["variances"] for i in table_config["labels"]]
  result["is_survey"] = table_config.get("is_survey", False)
  result["has_change"] = table_config.get("has_change", False)

  # Some tables (like "age" in demographic conditions) don't have the same "measures" values for all of the non-denom rows.
  # "age" has COUNTs for the first three but MEDIANs for the final row. This check allows you to set table_config["measures"] to
  # a 2d array when you need to explicitly pass different values for each non-denom row
  if isinstance(table_config["measures"][0],list):
    result["measures"] = table_config["measures"]
  else:
    result["measures"] = [table_config["measures"] for i in table_config["labels"]]
  if "suffixes" in table_config:
    result["suffixes"] = table_config["suffixes"]
  variables = [variable for variable in table_config["variables"]]
  if "headers" in table_config:
    result["headers"] = table_config["headers"]
  else:
    result["headers"] = get_headers(result["measures"][0])

  # If denominator is present, set a flag on result and prepend its label, measures, and variances
  if "denominator" in table_config:
    result["has_denominator"] = True
    result["labels"].insert(0, table_config["denominator"]["label"])
    result["measures"].insert(0, table_config["denominator"]["measures"])
    result["variances"].insert(0, table_config["denominator"]["variances"])
    variables.insert(0, table_config["denominator"]["variable"])
    if "placeholder" in table_config["denominator"]:
      result["denominator_placeholder"] = table_config["denominator"]["placeholder"]
  else:
    result["has_denominator"] = False

  if "has_change" in table_config and table_config["has_change"] == True:
    # print(merge(table_config["change"]["denominator"], table_config["denominator"])["label"])
    result["change"] = {
      "measures": [get_change_measures(measures) for measures in result["measures"]],
      "variances": [get_change_variances(variances) for variances in result["variances"]],
      "headers": get_change_headers(result["measures"][0])
    }

  result["upper_limits"] = [[None for j in result["labels"]] for i in result["vintages"]]
  result["lower_limits"] = [[None for j in result["labels"]] for i in result["vintages"]]
  if "upper_limits" in table_config:
    result["upper_limits"] = table_config["upper_limits"]
  if "lower_limits" in table_config:
    result["lower_limits"] = table_config["lower_limits"]

  if "scales" in table_config:
    result["scales"] = table_config["scales"]

  if "placeholder" in table_config and len(table_config["placeholder"]) > 0:
    result["placeholder"] = table_config["placeholder"]
  result["cells"] = []
  for i in range(len(table_config["vintages"])):
    column_vintage = []
    is_change = True if i == len(table_config["vintages"]) - 1 and result["has_change"] == True else False

    # Some indicators contain data where the vintage is a part of the column name. In those cases,
    # table_config should have an array of strings called "years" where each string is the "year" token
    # for each vintage. The indices of "years" match up those in "vintages"
    year = ""
    if is_change == True:
      year = "_change"
    elif "years" in table_config:
      year = table_config["years"][i]

    # variables is an array of strings containing the "variable" token for each row in the table, including the one for
    # the denominator, if there is one
    for j, variable in enumerate(variables):
      # Some tables require us to add tokens to the "variable" token from one cell to the next across the same
      # row. "suffixes" is an optional property in table config that allows us to add those by mapping strings to cells
      # within a row, similar to how measures and variances are mapped in
      # If the table_config doesn't specify suffixes, create an array of empty strings with the same length as
      # measures and variances
      suffixes = None
      if "suffixes" in table_config:
        suffixes = table_config["suffixes"]
      # Most tables belong to a given "subcategory" (ex: edu, health, etc) that is communicated by a token that comes at
      # the beginning of the token.
      _subcategory = ""
      _subgroup = subgroup
      if j == 0 and "denominator" in table_config:
        if "subcategory" in table_config["denominator"]:
          _subcategory = table_config["denominator"]["subcategory"]
        # The "Mutually Exclusive Race/Hispanic Origin" table's denominator shows the total population "pop" column,
        # even when viewing the page for a subgroup so we need a special flag to make sure the code doesn't
        # insert the subgroup token in those cases
        if "ignore_subgroup" in table_config["denominator"] and table_config["denominator"]["ignore_subgroup"] == True:
          _subgroup = "tot"
      elif "subcategory" in table_config:
        _subcategory = table_config["subcategory"]
      if is_change == True:
        column_vintage.append(build_row_column_names(variable, _subgroup, result["change"]["measures"][j], result["change"]["variances"][j], year, _subcategory, suffixes))
      else:
        column_vintage.append(build_row_column_names(variable, _subgroup, result["measures"][j], result["variances"][j], year, _subcategory, suffixes))
    result["cells"].append(column_vintage)
  return result


# "headers" defines the header cells a table should have in any given vintage, except for the top
# header row displaying the vintages (that value comes directly from table_config["vintages"])
# headers should be a three dimensional array
# The outer-most array should have one item (an array) per row of header cells
# The second-level arrays should have one item (an array) per cell in the row
# The inner-most arrays should always have two items - a string of the text that goes in the cell
# and a number that is the colspan for that cell
# This function compiles that 3D array in lists of objects with "label", and "colspan" properties
# for the front end
def build_column_headers(headers):
  output = []
  for row in headers:
    new_row = []
    for label, colspan in row:
      new_row.append({"label": label, "colspan": colspan})
    output.append(new_row)

  return output


def build_cell(value, measure, variance):
  new_cell = {}
  if np.isnan(value):
    new_cell["value"] = None
  else:
    new_cell["value"] = value
  new_cell["measure"] = measure
  new_cell["variance"] = variance
  return new_cell


# For rows in change over time vintages, we have to calculate
# reliability for cells belonging to each "measure" given for
# the row. For instance, cells for "percent" and "percent" moe may be reliable
# while cells for "percentage point" and "percentage point moe" for the same
# row are not. This function takes a value and its moe and calculates the cv,
# returning false (unreliable) if either value is nan or if the cv is >= 20
def get_change_reliability(value, moe):
  if np.isnan(value) or np.isnan(moe) or value == 0:
    return False
  return ((moe/1.645)/(abs(value)))*100 < 20


def build_row(label, values, measures, variances, upper_limit = None, lower_limit = None, is_change = False, is_survey = False, scale = None, is_denominator = False):
  new_row = {
    "label": label,
    "isDenominator": is_denominator,
    "cells": []
  }
  for k in range(len(values)):
    new_cell = build_cell(values[k], measures[k], variances[k])
    if k == 0 and upper_limit is not None and values[k] == upper_limit:
      new_cell["coding"] = "TOP"
    if k == 0 and lower_limit is not None and values[k] == lower_limit:
      new_cell["coding"] = "BOTTOM"
    if new_cell["measure"] not in ["PERCENT","PERCENTAGE_POINT"] and new_cell["variance"] != "CV" and scale != None:
      new_cell["scale"] = scale
    new_row["cells"].append(new_cell)

  if is_survey:
    if is_change:
      measure_set = set(measures)
      reliability_map = {}
      for measure in measure_set:
        reliability_map[measure] = get_change_reliability(*tuple([values[i] for i, var in enumerate(measures) if var == measure]))
      for cell in new_row["cells"]:
          cell["isReliable"] = reliability_map[cell["measure"]]
    else:
      if "CV" in variances:
        cv = values[variances.index("CV")]
        is_reliable = True
        if np.isnan(cv) or cv >= 20:
          is_reliable = False
        for cell in new_row["cells"]:
          cell["isReliable"] = is_reliable


  return new_row


def build_placeholder_row(label, placeholder, is_denominator = False):
  new_row = {
    "label": label,
    "isDenominator": False,
    "cells": None,
    "placeholder": placeholder,
    "isDenominator": is_denominator
  }
  return new_row


# Table config takes a list of table config objects and a row of a DataFrame (a pandas Series)
# It builds the final output to be consumed by the front end for a given geography, geoid, category combination
def build_vintages(table_list, df_row):
  result = []
  for table_config in table_list:
    indicator = {}
    indicator["title"] = table_config.get("title", "")
    indicator["id"] = table_config.get("id", None)
    indicator["isSurvey"] = table_config["is_survey"]
    indicator["vintages"] = []
    for vintage_index, vintage_label in enumerate(table_config["vintages"]):
      new_vintage = {}
      is_change_vintage = True if vintage_index == len(table_config["vintages"]) - 1 and table_config["has_change"] == True else False
      new_vintage["isChange"] = is_change_vintage
      new_vintage["label"] = vintage_label
      new_vintage["headers"] = build_column_headers(table_config["change"]["headers"]) if is_change_vintage else build_column_headers(table_config["headers"])
      new_vintage["rows"] = []
      vintage_cells = table_config["cells"][vintage_index]
      file_for_vintage = table_config["files"][vintage_index]
      if is_change_vintage == True:
        for row_index, cells_row in enumerate(vintage_cells):
          # READ THIS - There is an issue with how the change over time data files are generated where
          # columns are not added to the file if all of the values for the column are null
          # This code checks that all the column names exist before trying to get the data out of the dataframe
          # and adds columns where they are not found with np.nan for the value of every row, and prints a statement
          # because this is bound to cause hard to debug issues in the future. Note that this is only done here
          # for change over time data files because this issue isn't present in the regular files - those files
          # have columns for all of the values as they should, even if every value is null.
          for column_name in cells_row:
            if column_name not in df_row[file_for_vintage].index:
              print(f'COULD NOT FIND COLUMN {column_name} IN CHANGE FILE {file_for_vintage}. ADDING COLUMN WITH NAN VALUES')
              df_row[file_for_vintage,column_name] = np.nan
          values = df_row[file_for_vintage][cells_row].to_list()
          scale = None
          # "scales" is an optional property on table config. In this context, a "scale" refers to the mathematical term
          # for the number of digits after the decimal that a given float has (for more explanation, see https://math.stackexchange.com/a/1628229)
          # "scales" is an array with one integer for each row in the table (including denominator row if there is one) where the number
          # for the given row is the scale for datapoints in that row that don't have a variance of "CV" or a "measure" of "PERCENT"
          # or "PERCENTAGE_POINT". This allows us to arbitrarily assign scales to datapoints such as median age. Note that datapoints
          # with variance of "CV" and datapoints with measure of "PERCENT" or "PERCENTAGE_POINT" will not be assigned a "scale" value
          # but the front end always shows them with one digit after the decimal. Other data points (those for counts, medians, rates, etc)
          # will show up on the front end with no digits after the decimal if "scales" is not set.
          if "scales" in table_config:
            scale = table_config["scales"][row_index]
          new_row = build_row(
            table_config["labels"][row_index],
            values,
            table_config["change"]["measures"][row_index],
            table_config["change"]["variances"][row_index],
            is_change=is_change_vintage,
            is_survey=table_config["is_survey"],
            scale=scale
          )
          new_vintage["rows"].append(new_row)
      else:
        for row_index, cells_row in enumerate(vintage_cells):
          new_row = {}
          is_denominator = True if row_index == 0 and table_config["has_denominator"] == True else False
          if is_denominator and "denominator_placeholder" in table_config and len(table_config["denominator_placeholder"]) > 0:
            new_row = build_placeholder_row(table_config["labels"][row_index], table_config["denominator_placeholder"], is_denominator)
          elif is_denominator and "placeholder" in table_config and len(table_config["placeholder"]) > 0 and "denominator_placeholder" not in table_config:
            new_row = build_placeholder_row(table_config["labels"][row_index], table_config["placeholder"], is_denominator)
          elif not is_denominator and "placeholder" in table_config and len(table_config["placeholder"]) > 0:
            new_row = build_placeholder_row(table_config["labels"][row_index], table_config["placeholder"], is_denominator)
          else:
            values = df_row[file_for_vintage][cells_row].to_list()
            scale = None
            if "scales" in table_config:
              scale = table_config["scales"][row_index]
            new_row = build_row(
              table_config["labels"][row_index],
              values,
              table_config["measures"][row_index],
              table_config["variances"][row_index],
              table_config["upper_limits"][vintage_index][row_index],
              table_config["lower_limits"][vintage_index][row_index],
              is_change=is_change_vintage,
              is_survey=table_config["is_survey"],
              scale=scale,
              is_denominator=is_denominator
            )
          new_vintage["rows"].append(new_row)
      indicator["vintages"].append(new_vintage)
    result.append(indicator)
  return result


###
# MAIN EXECUTION
###

def main():
  # Some of the tables we have to display on the front end show values that don't actually map to any columns in the source data from EDM
  # This usually happens in rows that are the "denominator" of the table so logically don't have data for percent and percent moe
  # When this happens, the build_config function will build column names that don't actually exist in the csv we are looking at
  # This variable gives us a way to append columns to the csvs we load in with "hard coded" values
  # This variable is a dictionary whose keys are filenames of the csvs EDM gives us. The values are arrays of tuples. The first
  # item of each tuple is the column name you want to add to the file, the second item is the value for that column
  appended_columns = {
    "housing_security_puma": [("units_occurental_pct", 100),("units_occurental_pct_moe", 0), ("units_occu_pct", 100), ("units_occu_pct_moe", 0), ("housing_lottery_leases_pct", 100), ("housing_lottery_applications_pct", 100)],
    "housing_security_borough": [("units_occurental_pct", 100),("units_occurental_pct_moe", 0), ("units_occu_pct", 100), ("units_occu_pct_moe", 0), ("housing_lottery_leases_pct", 100), ("housing_lottery_applications_pct", 100)],
    "housing_security_citywide": [("units_occurental_pct", 100),("units_occurental_pct_moe", 0), ("units_occu_pct", 100), ("units_occu_pct_moe", 0), ("housing_lottery_leases_pct", 100), ("housing_lottery_applications_pct", 100)],
    "housing_production_puma": [("units_hi_newconstruction_count",0),("units_hi_preservation_count",0),("total_pct", 100)],
    "housing_production_borough": [("units_hi_newconstruction_count",0),("units_hi_preservation_count",0),("total_pct", 100)],
    "housing_production_citywide": [("units_hi_newconstruction_count",0),("units_hi_preservation_count",0),("total_pct", 100)],
  }

  for year in ["_2000","_0812","_1923"]:
    for geography in ["_puma","_borough","_citywide"]:
      columns = []
      for subgroup in ["","_anh","_bnh","_wnh","_hsp"]:
        columns = columns + [(f'age_median{subgroup}{var}', np.nan) for var in ["_pct","_pct_moe"]]
      appended_columns[f'demographics{year}{geography}'] = columns

  # In some cases we need to add a column to a data file that is a copy of an existing column, just with a different name. This should
  # be a last resort when the column names output by our code don't map exactly to existing columns

  # Because subgroup tokens such as "anh" and "bnh" are treated as "variables" in the "Mutually Exclusive Race/Hispanic Origin" table,
  # the column names our code generates don't match the values in the change over time data files (which do follow the normal convention)
  demo_change_copy = {}
  for subgroup in ["anh","bnh","wnh","hsp", "onh"]:
    for measure in ["count","pct","pnt"]:
      demo_change_copy[f'pop_change_{subgroup}_{measure}'] = f'pop_{subgroup}_change_{measure}'

  copy_columns = {
    "demo_change_puma": demo_change_copy,
    "demo_change_borough": demo_change_copy,
    "demo_change_citywide": demo_change_copy
  }

  ### Build Table Configs
  config = {
    "demo": load_json("../config/demo.json"),
    "econ": load_json("../config/econ.json"),
    "hsaq": load_json("../config/hsaq.json"),
    "hopd": load_json("../config/hopd.json"),
    "qlao": load_json("../config/qlao.json")
  }

  resolved_tables = {
    "district": {
      "hsaq": {
        "tot": [],
        "anh": [],
        "bnh": [],
        "wnh": [],
        "hsp": []
      }
    },
    "borough": {
      "hsaq": {
        "tot": [],
        "anh": [],
        "bnh": [],
        "wnh": [],
        "hsp": []
      }
    },
    "citywide": {
      "hsaq": {
        "tot": [],
        "anh": [],
        "bnh": [],
        "wnh": [],
        "hsp": []
      }
    }
  }

  # Loops through configuration files and saves result to /generated/resolved_table_configs.json with a fully
  # formed table config for every indicator, for every geography, for every subgroup
  for category, table_list in config.items():
    for table_config in table_list:
      base = table_config["base"].copy()
      for geography, geography_config in table_config["geographies"].items():
        geo_config = geography_config.copy()
        if geography not in resolved_tables:
          resolved_tables[geography] = {}
        if category not in resolved_tables[geography]:
          resolved_tables[geography][category] = {}
        base = table_config["base"]
        merged_table_config = merge(base, geo_config)
        for subgroup, subgroup_config in table_config["subgroups"].items():
          sub_config = subgroup_config.copy()
          if subgroup not in resolved_tables[geography][category]:
            resolved_tables[geography][category][subgroup] = []
          final = merge(merged_table_config, sub_config)
          resolved_tables[geography][category][subgroup].append(final)

  with open(f'../generated/resolved_table_configs.json', 'w') as fp:
      simplejson.dump(resolved_tables, fp, ignore_nan=True, indent=2)

  ### Build Page Configs
  resolved_tables = load_json('../generated/resolved_table_configs.json')
  output = {}
  for geography, geography_config in resolved_tables.items():
    if geography not in output:
      output[geography] = {}
    for category, category_config in geography_config.items():
      if category not in output[geography]:
        output[geography][category] = {}
      for subgroup, subgroup_config in category_config.items():
        if subgroup not in output[geography][category]:
          output[geography][category][subgroup] = []
        for table_config in subgroup_config:
          output[geography][category][subgroup].append(build_config(table_config, subgroup))

  with open(f'../generated/resolved_pages.json', 'w') as fp:
      simplejson.dump(output, fp, ignore_nan=True)

  ### Build and output final JSON files
  # resolved_pages is an object with a key for each geography
  # each geography is an object with a key for each category
  # each category is an object with a key for each subgroup (including "tot" for total pop)
  # each subgroup is a an array of objects, with an object for each indicator in that given category
  # each of those objects contains all the information needed to look up the data and build
  # the JSON necessary to display that table for every geoid in the geography
  output = {}
  with open('../generated/resolved_pages.json', 'r') as pages_file:
      pages = json.load(pages_file)
      for geography, geography_config in pages.items():
        if geography not in output:
          output[geography] = {}
        for category, category_config in geography_config.items():
          if category not in output[geography]:
            output[geography][category] = {}
          # First, download all csv's necessary to have all data for the given
          # geography an category, and combine them into one DataFrame
          # with a multi-index on the column axis that conveys filename
          df = pd.DataFrame()
          for subgroup, table_list in category_config.items():
            for table_config in table_list:
              if "files" in table_config and "placeholder" not in table_config:
                for file in table_config["files"]:
                  if df.empty:
                    df = download_df(geography, category, file, appended_columns, copy_columns)
                  else:
                    if file not in df.columns.levels[0].tolist():
                      df_for_file = download_df(geography, category, file, appended_columns, copy_columns)
                      df = df.merge(df_for_file, left_index=True, right_index=True)
          # Iterate through each row in the combined DataFrame
          for _geoid, df_row in df.iterrows():
            geoid = _geoid
            if geography == "citywide":
              geoid = "nyc"
            if geography == "borough":
              geoid = borough_map[geoid]
            if geoid not in output[geography][category]:
              output[geography][category][geoid] = {}
            area = output[geography][category][geoid]
            # For each geoid, call build_vintages to build the final output for that geoid and category
            for subgroup, table_list in category_config.items():
              if subgroup not in area:
                area[subgroup] = build_vintages(table_list, df_row)


  # Finally, iterate through geoid in the output object and save that data to a file
  # containing all data for the given geoid and category. Each file will be a object
  # with a key for each subgroup (including "tot"). Each of those will be an array
  # of objects for the tables in that subgroup for the geoid

  for geography, categories in output.items():
    for category, areas in categories.items():
      for geoid, data in areas.items():
        with open(f'../{OUTPUT_FOLDER}/{geography}_{geoid}_{category}.json', 'w') as fp:
          simplejson.dump(data, fp, ignore_nan=True)


if __name__ == "__main__":
  main()
