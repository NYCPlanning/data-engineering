import warnings

warnings.filterwarnings("ignore")

import pandas as pd  # noqa: E402
import rpy2.robjects.packages as rpackages  # noqa: E402
from statistical.variance_measures import variance_measures  # noqa: E402

survey_package = rpackages.importr("survey")
base = rpackages.importr("base")

from rpy2.robjects import pandas2ri  # noqa: E402

pandas2ri.activate()


def calculate_fractions(
    data: pd.DataFrame,
    variable_col,
    categories,
    rw_cols,
    weight_col,
    geo_col,
    add_MOE,
    keep_SE,
    race_crosstab=None,
):
    """This adds to dataframe so it should receive copy of data
    Parent category is only used in crosstabs, this is the original variable being crosstabbed on.
    """
    all_fractions = pd.DataFrame(index=data[geo_col].unique())
    for category in categories:
        category_col = f"{variable_col}_{category}"
        data.loc[:, category_col] = (data[variable_col] == category).astype(int)
        survey_design = survey_package.svrepdesign(
            variables=data[[category_col]],
            repweights=data[rw_cols],
            weights=data[weight_col],
            combined_weights=True,
            type="other",
            scale=4 / 80,
            rscales=1,
        )
        single_fraction: pd.DataFrame = survey_package.svyby(
            formula=data[category_col],
            by=data[[geo_col]],
            design=survey_design,
            FUN=survey_package.svymean,
            vartype=base.c("se", "ci", "var"),
        )
        variance_measures(single_fraction, add_MOE)
        single_fraction.drop(columns=[geo_col], inplace=True)
        if race_crosstab is None:
            columns = [
                f"{category}_pct",
                f"{category}_pct_se",
                # f"{category}_pct_cv",
                f"{category}_pct_moe",
                f"{category}_pct_denom",
            ]
        else:
            columns = [
                f"{category}_{race_crosstab}_pct",
                f"{category}_{race_crosstab}_pct_se",
                # f"{category}_{race_crosstab}_pct_cv",
                f"{category}_{race_crosstab}_pct_moe",
                f"{category}_{race_crosstab}_pct_denom",
            ]

        denom = data.groupby(geo_col).sum()[weight_col]
        single_fraction["denominator"] = denom
        single_fraction.rename(
            columns={
                "V1": columns[0],
                "se": columns[1],
                # "cv": columns[2],
                "moe": columns[2],
                "denominator": columns[3],
            },
            inplace=True,
        )
        # print(single_fraction)
        single_fraction = single_fraction.apply(
            SE_to_zero_no_respondents, axis=1, result_type="expand"
        )
        all_fractions = all_fractions.merge(
            single_fraction[columns], left_index=True, right_index=True
        )

    if not keep_SE:
        remove_SE(all_fractions)

    return all_fractions


def SE_to_zero_no_respondents(geography):
    """If fraction is zero then no respodents in this category for this geography.
    In this situation variance measures should be set to null"""
    if not geography.iloc[0]:
        geography.iloc[1:3] = None
    return geography


def remove_SE(df: pd.DataFrame):
    df.drop(columns=[c for c in df.columns if c[-3:] == "_se"], inplace=True)
    return df
