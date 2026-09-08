import argparse
import os
from pathlib import Path

from dcpy.lifecycle.builds import load, plan


def parse_args() -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-g", "--geography", type=str, help="The geography year, e.g. 2010_to_2020"
    )
    args = parser.parse_args()
    return args.geography


if __name__ == "__main__":
    geography = parse_args()
    lockfile = plan.plan(Path(__file__).parent / geography / "recipe.yml")
    load.load_source_data_from_resolved_recipe(lockfile)
    os.system(f"bash geolookup/{geography}/run.sh")
