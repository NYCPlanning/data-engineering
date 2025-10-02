import pandas as pd

from dcpy.connectors.edm import publishing
from dcpy.lifecycle import builds


def get_latest_source_data_versions(product: str) -> pd.DataFrame:
    """Gets latest available versions of source datasets for specific data product
    Does NOT return versions used in any specific build
    TODO this should not come from publishing, but should be defined in code for each data product
    """
    source_data_versions = publishing.get_source_data_versions(
        publishing.PublishKey(product, "latest")
    )

    recipes_connector = builds.get_recipes_default_connector()
    source_data_versions["version"] = source_data_versions.index.map(
        recipes_connector.get_latest_version
    )
    return source_data_versions
