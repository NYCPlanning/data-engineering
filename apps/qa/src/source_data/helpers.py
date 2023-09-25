from typing import Dict

from src import ROOT_PATH
from dcpy.connectors.edm import recipes


def get_dataset_versions(product: str) -> Dict[str, list[str]]:
    recipe = recipes.recipe_from_yaml(
        ROOT_PATH / "products" / product.lower() / "recipe.yml"
    )
    return {
        dataset.name: recipes.get_all_versions(dataset.name)
        for dataset in recipe.inputs.datasets
    }
