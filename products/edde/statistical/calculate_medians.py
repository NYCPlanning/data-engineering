import random
import warnings

warnings.filterwarnings("ignore")

import pandas as pd  # noqa: E402
import rpy2.robjects.packages as rpackages  # noqa: E402

survey_package = rpackages.importr("survey")
base = rpackages.importr("base")

from rpy2.robjects import pandas2ri  # noqa: E402
from statistical.variance_measures import variance_measures  # noqa: E402

pandas2ri.activate()


def get_design_object(data, variable_col, rw_cols, weight_col):
    survey_design = survey_package.svrepdesign(
        variables=data[[variable_col]],
        repweights=data[rw_cols],
        weights=data[weight_col],
        combined_weights=True,
        type="other",
        scale=4 / 80,
        rscales=1,
    )

    return survey_design


def calculate_median(
    data: pd.DataFrame, variable_col, rw_cols, weight_col, geo_col, add_MOE, keep_SE
):
    """Became attribute and now gets aggregator passed as first variable which I don't want"""
    survey_design = get_design_object(data, variable_col, rw_cols, weight_col)
    aggregated: pd.DataFrame
    aggregated = survey_package.svyby(
        formula=data[[variable_col]],
        by=data[[geo_col]],
        design=survey_design,
        quantiles=base.c(0.5),
        FUN=survey_package.svyquantile,
        vartype=base.c("se", "ci", "var", "cv"),
        **{"interval.type": "quantile"},
    )
    columns = [
        f"{variable_col}-median",
        f"{variable_col}-median-se",
        f"{variable_col}-median-cv",
    ]
    aggregated.rename(
        columns={
            variable_col: columns[0],
            f"se.{variable_col}": columns[1],
            f"cv.{variable_col}": columns[2],
        },
        inplace=True,
    )
    aggregated = aggregated[columns]
    aggregated = variance_measures(aggregated, add_MOE, keep_SE)
    return aggregated


def calculate_median_with_crosstab(
    data: pd.DataFrame,
    variable_col,
    crosstab_col,
    rw_cols,
    weight_col,
    geo_col,
    add_MOE,
    keep_SE,
    second_crosstab_name=None,
):
    """Can only do one crosstab at a time for now"""
    # data[rw_cols] = data[rw_cols].apply(axis=1, func=random_rw, result_type="expand")
    data[rw_cols] = data[rw_cols].replace({0: 0.01})
    survey_design = get_design_object(data, variable_col, rw_cols, weight_col)
    aggregated = survey_package.svyby(
        formula=data[[variable_col]],
        by=data[[geo_col, crosstab_col]],
        design=survey_design,
        quantiles=base.c(0.5),
        FUN=survey_package.svyquantile,
        vartype=base.c("se", "ci", "var", "cv"),
        **{"interval.type": "quantile"},
    )

    if second_crosstab_name:
        data_point_label = f"{variable_col}-{second_crosstab_name}"
    else:
        data_point_label = variable_col

    median_col_name = f"{data_point_label}-median"
    se_col_name = f"{data_point_label}-median-se"
    cv_col_name = f"{data_point_label}-median-cv"
    aggregated.rename(
        columns={
            variable_col: median_col_name,
            f"se.{variable_col}": se_col_name,
            f"cv.{variable_col}": cv_col_name,
        },
        inplace=True,
    )
    pivot_table = pd.pivot_table(
        data=aggregated,
        values=[median_col_name, se_col_name, cv_col_name],
        columns=crosstab_col,
        index=geo_col,
    )
    pivot_table.columns = [
        f"{crosstab_var}-{stat}" for stat, crosstab_var in pivot_table.columns
    ]
    pivot_table = variance_measures(pivot_table, add_MOE, keep_SE)

    return pivot_table


def random_rw(row):
    rv = []
    for i in range(1, 81):
        rv.append(random.randint(1, 100))
    return rv
