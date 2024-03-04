from dcpy.connectors.edm import recipes
from . import PARQUET_PATH, models


# doesn't strictly need to be recipe config, but at least config-like object
def archive_raw_dataset(dataset: str, version: str | None = None) -> recipes.Config:
    """Given dataset and potentially a semantic version, ingest it from its source and archive it in its
    raw format, with metadata, in edm-recipes/raw_data"""
    import_definition = models.read_import_definition(dataset)

    ## logic to grab dataset based on ImportDefinition, dump locally

    ## logic to "compute" recipes.Config object, similar to Config.compute in library
    ## should be limited to metadata gathering - version, etc
    ## leaving here as opposed to helper function because not sure if it will only depend on import definition
    ##   or read any data from downloaded dataset

    ## logic to push local dataset and "computed" config to raw archive in s3

    ## return Config, from which s3path should be computed

    raise NotImplemented


def transform_to_parquet(config: recipes.Config):
    """Given config of archived raw dataset, transform it to parquet"""
    ### idea is that this will dump to PARQUET_PATH - other functions will assume parquet file is there as well
    raise NotImplemented


def validate_dataset(config: recipes.Config):
    """Given config of imported dataset, validate data expectations/contract"""
    raise NotImplemented
