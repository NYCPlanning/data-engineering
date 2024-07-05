import importlib
from pandas import DataFrame
from pathlib import Path
import shutil

from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    S3Source,
    ScriptSource,
    Source,
    Template,
)
from dcpy.models.connectors import socrata, web as web_models
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.models.lifecycle.builds import Recipe
from dcpy.utils import s3
from dcpy.utils.logging import logger
from dcpy.connectors.edm import publishing
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors import web

from dcpy.lifecycle.ingest.configure import read_template
from dcpy.builds.plan import recipe_from_yaml, DEFAULT_RECIPE


def download_file_from_source(
    source: Source, filename: str, version: str, dir: Path
) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    path = dir / filename
    match source:
        ## Non reqeust-based methods
        case LocalFileSource():
            if source.path != path:
                shutil.copy(source.path, path)
        case GisDataset():
            publishing.download_gis_dataset(source.name, version, dir)
        case S3Source():
            s3.download_file(source.bucket, source.key, path)
        case ScriptSource():
            module = importlib.import_module(
                f"dcpy.connectors.{source.connector}.{source.function}"
            )
            logger.info(f"Running custom ingestion script {source.function}.py")
            df: DataFrame = module.extract()
            df.to_parquet(path)

        ## request-based methods
        case web_models.FileDownloadSource():
            web.download_file(source.url, path)
        case web_models.GenericApiSource():
            web.download_file(source.endpoint, path)
        case socrata.Source():
            extract_socrata.download(source, path)
        case _:
            raise NotImplementedError(
                f"Source type {source.type} not supported for download_file_from_source"
            )


def update_recipe_input_datasets(dataset: str) -> None:
    """
    This function needs to be run from product repo with recipe file in it.

    Extract input datasets for provided dataset. Only certain input datasets can be extracted
    automatically: they are usually of api/socrata source type. The rest should be extracted manually
    (refer to individual recipe files).

    dataset: product name. For example: db-pluto
    """

    # read in product recipe. Note, this fn needs to be called from product's dir
    product_recipe: Recipe = recipe_from_yaml(Path(DEFAULT_RECIPE))

    for input_ds in product_recipe.inputs.datasets:
        input_ds_recipe: Template = read_template(dataset=input_ds.name)

        # skip dataset if version is present
        if input_ds.version_env_var or input_ds.version:
            logger.warn(
                f"❗️ Skipped: {input_ds.name} has a specified version in the recipe. Please extract this dataset manually."
            )
            continue

        match input_ds_recipe.source:
            # These source types don't require input info for their extraction and can be automated via this fn.
            case GisDataset() | socrata.Source() | web_models.GenericApiSource():
                try:
                    logger.info(f"✅ Success: {input_ds.name}")
                except Exception as e:
                    logger.error(
                        f"❌ Failure: {input_ds.name}. Please extract this dataset manually."
                    )
                    print(f"Error message: {e}")
            case _:
                logger.warn(
                    f"❗️ Skipped: {input_ds.name} has unsupported source type. Please extract this dataset manually."
                )
