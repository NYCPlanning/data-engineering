from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import matplotlib as mpl
import matplotlib.pyplot as plt
from typing import Dict
import re
import streamlit as st

datasets = {
    "dcp_cb2020": ["2023-09-05", "2023-08-05"],
    "dot_bridges": ["2023-07-15"],
    "dcp_zoningtaxlots": ["2023-06-01", "2023-07-01", "2023-08-01", "2023-09-01"],
    "nypd_cars": ["2023-08-15", "2023-07-15"],
    "bpl_libraries": ["2023-08-15", "2023-06-15"],
}


def plot_series(datasets: Dict[str, list[str]], months_back: int = 3):
    end_date = date.today()
    start_date = end_date - relativedelta(months=months_back)

    n = len(datasets)

    DARK_GREY = "#282828"
    LIGHT_GREY = "#909090"
    LINES = "#0784b580"
    mpl.rcParams["text.color"] = LIGHT_GREY
    mpl.rcParams["axes.labelcolor"] = LIGHT_GREY
    mpl.rcParams["xtick.color"] = LIGHT_GREY
    mpl.rcParams["ytick.color"] = LIGHT_GREY

    fig, ax = plt.subplots(figsize=(10, 4))
    fig.set_figheight(n / 4)
    for index, dataset in enumerate(datasets):
        versions = datasets[dataset]
        versions = [
            datetime.strptime(version, "%Y%m%d").date()
            for version in versions
            if re.match("^\d{8}$", version)
        ]
        versions = [
            version
            for version in versions
            if version > start_date and (version < end_date)
        ]
        ax.plot(
            versions,
            [n - index - 1 for _ in versions],
            marker="o",
            markersize=(24 * 15 / n),
            linewidth=1.5,
            markeredgewidth=1.5,
            markerfacecolor=f"{DARK_GREY}B0",
            color=LINES,
        )

    fig.set_facecolor(DARK_GREY)
    ax.set_facecolor(DARK_GREY)
    ax.set_axisbelow(True)

    ax.yaxis.grid(color=LIGHT_GREY)
    ax.xaxis.grid(color=LIGHT_GREY)
    ax.set_ylim(bottom=-0.5, top=len(datasets) - 0.5)

    months = mpl.dates.MonthLocator()
    ax.xaxis.set_major_locator(months)
    year_month = mpl.dates.DateFormatter("%Y %b")
    ax.xaxis.set_major_formatter(year_month)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    ### tell matplotlib the ticks and labels to use on Y-axis
    ax.set_yticks(range(len(datasets)))
    ax.set_yticklabels(list(datasets.keys().__reversed__()), fontsize=8)

    return st.pyplot(fig)
