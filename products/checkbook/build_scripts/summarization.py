import logging
from typing import Union

import pandas as pd

from . import (
    BUILD_OUTPUT_FILENAME,
    OUTPUT_DIR,
    SUMMARY_STATS_DESCRIBE_FILENAME,
    SUMMARY_STATS_LOG_FILENAME,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / SUMMARY_STATS_LOG_FILENAME),
        logging.StreamHandler(),
    ],
)


def _total_check_amounts(data: pd.DataFrame) -> float:
    return round(sum(data["check_amount"]), 2)


def _percent_of_total_records(
    selected_data: pd.DataFrame, all_data: pd.DataFrame
) -> float:
    return round((len(selected_data) / len(all_data)) * 100, 2)


def _percent_of_total_check_amount(
    selected_data: pd.DataFrame, all_data: pd.DataFrame
) -> float:
    return round(
        ((_total_check_amounts(selected_data) / _total_check_amounts(all_data)) * 100),
        2,
    )


def report_summary_stat(
    description: str,
    result: Union[int, float, pd.Series],
    result_format: str = ",",  # default to commas as thousands separators
) -> str:
    if isinstance(result, pd.Series):
        return f"{description}:\n{result}\n"
    return f"{description}:\n\t{result:{result_format}}"


def export_data_description(df: pd.DataFrame) -> None:
    logging.info("--- Column names and datatypes ---")
    logging.info(sorted(df.columns))
    logging.info(df.dtypes)
    logging.info("---")

    descriptive_stats = df.describe(include="all")
    descriptive_stats_numerical_columns = df.describe()
    descriptive_stats.to_csv(OUTPUT_DIR / SUMMARY_STATS_DESCRIBE_FILENAME)
    logging.info(
        "Saved descriptive statistics to {}/{}".format(
            OUTPUT_DIR, SUMMARY_STATS_DESCRIBE_FILENAME
        )
    )
    logging.info("Preview of descriptive statistics:")
    logging.info(descriptive_stats)
    logging.info("Preview of descriptive statistics for numerical columns only:")
    logging.info(descriptive_stats_numerical_columns)
    logging.info("---")


def geometries_summarization_statistics(df: pd.DataFrame) -> None:
    projects_with_geometry = df[df["has_geometry"]]

    summary_stat_reports = [
        report_summary_stat(
            description="# of projects",
            result=len(df),
        ),
        report_summary_stat(
            description="# of projects mapped to geometries",
            result=len(projects_with_geometry),
        ),
        report_summary_stat(
            description="% of projects mapped to geometries",
            result=_percent_of_total_records(projects_with_geometry, df),
        ),
        report_summary_stat(
            description="Total check amounts ($)",
            result=_total_check_amounts(df),
        ),
        report_summary_stat(
            description="Total check amounts mapped to geometries ($)",
            result=_total_check_amounts(projects_with_geometry),
        ),
        report_summary_stat(
            description="% of total check amounts mapped to geometries",
            result=_percent_of_total_check_amount(projects_with_geometry, df),
        ),
    ]

    for report in summary_stat_reports:
        logging.info(report)
    logging.info("---")


def categorization_summarization_statistics(df: pd.DataFrame) -> None:
    projects_with_geometry = df[df["has_geometry"]]

    summary_stat_reports = [
        report_summary_stat(
            description="Value counts: 'final_category'",
            result=df["final_category"].value_counts(),
        ),
        report_summary_stat(
            description="Value frequencies: 'final_category'",
            result=df["final_category"].value_counts(normalize=True),
        ),
        report_summary_stat(
            description="Value counts: 'final_category' for projects with geometry",
            result=projects_with_geometry["final_category"].value_counts(),
        ),
        report_summary_stat(
            description="Value frequencies: 'final_category' for projects with geometry",
            result=projects_with_geometry["final_category"].value_counts(
                normalize=True
            ),
        ),
    ]
    for report in summary_stat_reports:
        logging.info(report)
    logging.info("---")

    categories = df["final_category"].unique()
    for category in categories:
        category_projects = df[(df["final_category"] == category)]
        cat_geoms = df[df["has_geometry"] & (df["final_category"] == category)]

        count_projects = report_summary_stat(
            description=f"# projects with category '{category}'",
            result=len(category_projects),
        )
        percent_projects = report_summary_stat(
            description=f"% projects with category '{category}' and a geometry",
            result=_percent_of_total_records(cat_geoms, df),
        )
        percent_total_money = report_summary_stat(
            description=f"Total check amounts with category '{category}' and a geometry",
            result=_total_check_amounts(cat_geoms),
        )
        percent_total_money = report_summary_stat(
            description=f"% total check amounts with category '{category}' and a geometry",
            result=_percent_of_total_check_amount(cat_geoms, df),
        )
        summary_stat_reports_per_category = [
            count_projects,
            percent_projects,
            percent_total_money,
        ]
        for report in summary_stat_reports_per_category:
            logging.info(report)
    logging.info("---")


def categorization_run_summary_statistics():
    # format the display of large numbers in DataFrames to use commas and 2 decimal places
    pd.options.display.float_format = "{:,.2f}".format

    final_categorization_file = pd.read_csv(OUTPUT_DIR / BUILD_OUTPUT_FILENAME)
    export_data_description(final_categorization_file)
    geometries_summarization_statistics(final_categorization_file)
    categorization_summarization_statistics(final_categorization_file)


if __name__ == "__main__":
    logging.info("started summarization ...")
    categorization_run_summary_statistics()
    logging.info("finished summarization")
