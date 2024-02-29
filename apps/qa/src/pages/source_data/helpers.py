import os
from typing import Dict

from src import ROOT_PATH, APP_PATH

from dcpy.connectors.edm import recipes
from dcpy.builds import plan


def get_source_datasets(product: str) -> list[str]:
    root = APP_PATH if "DEPLOY" in os.environ else ROOT_PATH
    recipe = plan.recipe_from_yaml(root / "products" / product / "recipe.yml")
    return [dataset.name for dataset in recipe.inputs.datasets]


def get_dataset_metadata(product: str) -> Dict[str, list[recipes.ArchivalMetadata]]:
    datasets = get_source_datasets(product)
    metadata_df = recipes.get_logged_metadata(datasets)
    metadata: Dict[str, list[recipes.ArchivalMetadata]] = {}
    for index, row in metadata_df.iterrows():  ## group by
        m = recipes.ArchivalMetadata(**row.to_dict())
        dataset = row["name"]
        if dataset not in metadata:
            metadata[dataset] = []
        metadata[dataset].append(m)
    return metadata


def scrape_metadata(product: str):
    for dataset in get_source_datasets(product):
        recipes.scrape_metadata(dataset)
