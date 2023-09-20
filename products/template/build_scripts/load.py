from pathlib import Path
from dcpy.connectors.edm import recipes

from . import RECIPE_PATH, RECIPE_LOCK_PATH


def load_source_data():
    recipes.plan(
        Path(RECIPE_PATH),
        Path(RECIPE_LOCK_PATH),
    )
    recipes.import_datasets(Path(RECIPE_LOCK_PATH))


if __name__ == "__main__":
    load_source_data()
