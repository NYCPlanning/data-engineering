from datetime import date
from typing import Dict

import matplotlib as mpl
import matplotlib.dates as dates
import matplotlib.pyplot as plt
import streamlit as st
from dateutil.relativedelta import relativedelta

from dcpy.models import library


def plot_series(
    datasets: Dict[str, list[library.ArchivalMetadata]], months_back: int = 6
):
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
    fig.set_figheight(n / 2)
    for index, dataset in enumerate(datasets):
        metadata = [
            metadata
            for metadata in datasets[dataset]
            if metadata.timestamp.date() >= start_date
        ]
        timestamps = [m.timestamp for m in metadata]
        versions = [m.version for m in metadata]
        ax.plot(
            # mypy is not happy with list of datetimes, as this technically isn't ArrayLike
            # however, it is valid in pyplot
            timestamps,  # type: ignore
            [n - index - 1 for _ in versions],
            marker="o",
            markersize=12,
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

    months = dates.MonthLocator()
    ax.xaxis.set_major_locator(months)
    year_month = dates.DateFormatter("%Y %b")
    ax.xaxis.set_major_formatter(year_month)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    ### tell matplotlib the ticks and labels to use on Y-axis
    ax.set_yticks(range(len(datasets)))
    ax.set_yticklabels(list(datasets.keys().__reversed__()), fontsize=8)

    return st.pyplot(fig)
