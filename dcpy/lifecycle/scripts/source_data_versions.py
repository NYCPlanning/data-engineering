import pandas as pd

from dcpy.connectors.edm import recipes, publishing


def get_latest_source_data_versions(product: str) -> pd.DataFrame:
    """Gets latest available versions of source datasets for specific data product
    Does NOT return versions used in any specific build
    TODO this should not come from publishing, but should be defined in code for each data product
    """
    source_data_versions = publishing.get_source_data_versions(
        publishing.PublishKey(product, "latest")
    )
    source_data_versions["version"] = source_data_versions.index.map(
        recipes.get_latest_version
    )
    return source_data_versions
