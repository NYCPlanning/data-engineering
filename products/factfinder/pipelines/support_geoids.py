import argparse
import importlib
import os


def parse_args() -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-g", "--geography", type=str, help="The geography year, e.g. 2010_to_2020"
    )
    args = parser.parse_args()
    return args.geography


if __name__ == "__main__":
    # Get Geography year
    geography = parse_args()

    # Initialize pff instance
    AggregatedGeography = importlib.import_module(
        f"factfinder.geography.{geography}"
    ).AggregatedGeography()

    # Concatenate dataframes and export to 1 large csv
    output_folder = f".output/support_geoids/geography={geography}"
    os.makedirs(output_folder, exist_ok=True)
    AggregatedGeography.support_geoids.to_csv(
        f"{output_folder}/support_geoids.csv", index=False
    )
