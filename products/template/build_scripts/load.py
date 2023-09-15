import os
from pathlib import Path
import yaml
from dcpy.connectors.edm import recipes
from dcpy.utils.logging import logger

from . import RECIPE_PATH, RECIPE_LOCK_PATH


def load_source_data():
    recipe_lock = recipes.plan_recipe(Path(RECIPE_PATH))
    with open(RECIPE_LOCK_PATH, "w", encoding="utf-8") as f:
        logger.info(f"Writing recipe lockfile to {RECIPE_LOCK_PATH}")
        yaml.dump(recipe_lock, f)
    with open(RECIPE_LOCK_PATH, "r", encoding="utf-8") as f:
        recipe_lock = yaml.safe_load(f)
    logger.info("Importing Recipe")
    recipes.import_recipe_datasets(recipe_lock)


if __name__ == "__main__":
    load_source_data()
