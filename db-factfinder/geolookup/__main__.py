import argparse
import os


def parse_args() -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-g", "--geography", type=str, help="The geography year, e.g. 2010_to_2020"
    )
    args = parser.parse_args()
    return args.geography


if __name__ == "__main__":
    geography = parse_args()
    os.system(f"bash geolookup/{geography}/run.sh")
