"""Generalized code to get counts and associated variances"""

import warnings

warnings.filterwarnings("ignore")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import rpy2.robjects.packages as rpackages  # noqa: E402
from statistical.variance_measures import variance_measures  # noqa: E402

# from statistical.CV import

survey_package = rpackages.importr("survey")
base = rpackages.importr("base")

from rpy2.robjects import pandas2ri  # noqa: E402

pandas2ri.activate()


def calculate_counts(
    data: pd.DataFrame,
    variable_col,
    rw_cols,
    weight_col,
    geo_col,
    add_MOE,
    keep_SE,
    crosstab=None,
):
    data["a"] = 1
    data[rw_cols].replace({0: 0.01}, inplace=True)

    if crosstab:
        original_var = variable_col
        variable_col = f"{variable_col}_{crosstab}"
        data.loc[:, variable_col] = data[original_var] + "_" + data[crosstab]
        data[variable_col] = data[variable_col].replace({np.NaN: None})

    survey_design = survey_package.svrepdesign(
        variables=data[["a"]],
        repweights=data[rw_cols],
        weights=data[weight_col],
        combined_weights=True,
        type="other",
        scale=4 / 80,
        rscales=1,
    )
    aggregated = survey_package.svyby(
        formula=data["a"],
        by=data[[geo_col, variable_col]],
        design=survey_design,
        FUN=survey_package.svytotal,
        vartype=base.c("se", "ci", "var"),
    )
    aggregated = variance_measures(aggregated, add_MOE)
    aggregated.rename(
        columns={"V1": "count", "se": "count_se", "cv": "count_cv", "moe": "count_moe"},
        inplace=True,
    )

    pivot_table = pd.pivot_table(
        data=aggregated,
        values=["count", "count_se", "count_cv", "count_moe"],
        columns=variable_col,
        index=geo_col,
    )
    label_mapper = {
        "count": "",
        "count_se": "_se",
        "count_cv": "_cv",
        "count_moe": "_moe",
    }
    pivot_table.columns = [
        f"{var}{label_mapper[stat]}" for stat, var in pivot_table.columns
    ]
    counts_to_zero(pivot_table)
    if not keep_SE:
        remove_SE(pivot_table)
    return pivot_table


def counts_to_zero(df: pd.DataFrame):
    """If not respondents for a certain category live in a particular geography,
    convert corresponding value from NA to zero. For example there are were no black
    non-hispanic respondents in 3901 with limited english proficiency in 2019"""
    for c in df.columns:
        if c[-6:] == "_count":
            df[c].replace({np.NaN: 0}, inplace=True)


def remove_SE(df: pd.DataFrame):
    df.drop(columns=[c for c in df.columns if c[-3:] == "_se"], inplace=True)
    return df
