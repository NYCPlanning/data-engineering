"""Calculate medians using linear interpolation"""

import json

import numpy as np
import pandas as pd
from scipy import stats

z_score = stats.norm.ppf(0.95)


def calculate_median_LI(data, variable_col, geo_col, new_col_label=None):
    print(f"calculate median LI for {new_col_label}")
    bin_dict = lookup_metadata(variable_col, "ranges")
    if new_col_label is None:
        new_col_label = variable_col
    final = pd.DataFrame(index=data[geo_col].unique(), columns=["median", "moe"])
    geo_bin_counts = frequency_per_bin_geo(data, variable_col, geo_col, bin_dict)
    for puma, bin_counts in geo_bin_counts.groupby(level=0):
        bin_counts["cum_sum_below"] = (
            bin_counts.cumsum(axis=0).frequency - bin_counts.frequency
        )
        bin_counts = bin_counts.loc[puma]
        final.loc[puma] = calculate(
            bin_counts=bin_counts, bin_dict=bin_dict, indicator_name=variable_col
        )
    final["cv"] = (final["moe"] / z_score) / final["median"] * 100
    final.rename(
        columns={
            "median": f"{new_col_label}_median",
            "moe": f"{new_col_label}_median_moe",
            "cv": f"{new_col_label}_median_cv",
        },
        inplace=True,
    )
    final = final.astype(float)
    return final


def calculate(bin_counts, bin_dict, indicator_name):
    """Adopted from PFF. We have a different data structure, using a dataframe instead
    of a list."""
    N = bin_counts.frequency.sum()
    C = 0
    i = 0
    while C < N / 2 and i <= bin_counts.shape[0] - 1:
        # Calculate cumulative frequency until half of all units are accounted for
        i += 1
        C = bin_counts.iloc[i]["cum_sum_below"]
    i = i - 1
    median_bin = bin_counts.index[i]
    if i == 0 or C == 0 or i == bin_counts.shape[0] - 1:
        raise Exception("some corner case")
    else:
        print("Found N/2 bin in middle :>) ")
        lower_bound_of_median_containing_bin = bin_dict[median_bin][0]
        cum_sum_lower_bins = bin_counts.iloc[i]["cum_sum_below"]
        width_median_containing_bin = bin_dict[median_bin][1] - bin_dict[median_bin][0]
        frequency_of_median_containing_bin = bin_counts.iloc[i].frequency
        median = lower_bound_of_median_containing_bin + (
            (N / 2) - cum_sum_lower_bins
        ) * (width_median_containing_bin / frequency_of_median_containing_bin)
    MOE = calculate_MOE(bin_counts, indicator_name, median_bin, bin_dict)
    return median, MOE


def calculate_MOE(bin_counts, indicator_name, median_bin, bin_dict):
    B = bin_counts.frequency.sum()
    DF = lookup_metadata(indicator_name, "design_factor")

    se_50 = DF * ((93 / (7 * B)) * 2500) ** 0.5
    p_lower = 50 - se_50
    p_upper = 50 + se_50

    bin_counts["cum_pct"] = (
        bin_counts.cumsum(axis=0).frequency / bin_counts.frequency.sum()
    ) * 100

    lower_bin_index = [
        i for i in range(bin_counts.shape[0]) if bin_counts.iloc[i].cum_pct > p_lower
    ][0]
    upper_bin_index = [
        i for i in range(bin_counts.shape[0]) if bin_counts.iloc[i].cum_pct > p_upper
    ][0]

    lower_boundary = get_CI_boundary(
        p=p_lower, bin_counts=bin_counts, bin_index=lower_bin_index, bin_dict=bin_dict
    )

    upper_boundary = get_CI_boundary(
        p=p_upper, bin_counts=bin_counts, bin_index=upper_bin_index, bin_dict=bin_dict
    )
    return (upper_boundary - lower_boundary) * z_score / 2


def get_CI_boundary(p, bin_counts, bin_index, bin_dict):
    boundary_bin = bin_counts.index[bin_index]

    A1, A2 = bin_dict[boundary_bin]
    C1 = bin_counts.iloc[bin_index - 1]["cum_pct"]
    C2 = bin_counts.iloc[bin_index]["cum_pct"]
    return get_bound(p, A1, A2, C1, C2)


def get_bound(p, A1, A2, C1, C2):
    """Straight from PFF"""
    if ((C2 - C1) + A1) != 0:
        return (p - C1) * (A2 - A1) / (C2 - C1) + A1
    else:
        return np.nan


def frequency_per_bin_geo(PUMS: pd.DataFrame, indicator_name, geo_col, bins: dict):
    labels = []
    ranges = [0]
    for label, range in bins.items():
        labels.append(label)
        ranges.append(range[1])

    # cuts = [r[0] for r in ranges]
    PUMS["bin"] = pd.cut(
        PUMS[indicator_name], bins=ranges, labels=labels, include_lowest=True
    )
    gb = PUMS.groupby([geo_col, "bin"]).sum()[["PWGTP"]]
    return gb.rename(columns={"PWGTP": "frequency"})


def lookup_metadata(indicator_name, k):
    with open("resources/statistical/median_bins.json") as f:
        median_bins = json.load(f)
    dict = median_bins[indicator_name][k]

    return dict
