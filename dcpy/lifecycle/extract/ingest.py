from datetime import datetime
import importlib
import pandas as pd
import geopandas as gpd
import os
import shutil

from dcpy.utils import s3

from pathlib import Path
from urllib.parse import urlparse

from dcpy.models import file
from dcpy.models.lifecycle.extract import (
    LocalFileSource,
    ScriptSource,
    Template,
    Config,
)
from dcpy.models.connectors import socrata, web as web_models
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors import web
from . import TMP_DIR, PARQUET_PATH, metadata


def download_file_from_source(template: Template, version: str, dir=TMP_DIR) -> Path:
    """From parsed config template and version, download raw data from source
    and return path of saved local file."""
    match template.source:
        ## Non reqeust-based methods
        case LocalFileSource():
            return template.source.path
        case GisDataset():
            return publishing.download_gis_dataset(template.source.name, version, dir)
        case ScriptSource():
            module = importlib.import_module(
                f"dcpy.connectors.{template.source.connector}.{template.source.function}"
            )
            logger.info(f"Running custom ingestion script {template.name}.py")
            return module.runner(dir)

        ## request-based methods
        case web_models.FileDownloadSource():
            return web.download_file(
                template.source.url,
                os.path.basename(urlparse(template.source.url).path),
                dir,
            )
        case web_models.GenericApiSource():
            return web.download_file(
                template.source.endpoint,
                f"{template.name}.{template.source.format}",
                dir,
            )
        case socrata.Source():
            return web.download_file(
                extract_socrata.get_download_url(template.source),
                f"{template.name}.{template.source.extension}",
                dir,
            )


def extract_and_archive_raw_dataset(dataset: str, version: str | None) -> Config:
    """From dataset name and optional version,
    1. parse template
    2. determine version from source or set it as today's date if missing
    3. download raw file from source
    4. generate recipes config object
    5. archive raw dataset with config
    Returns config object."""
    # gather metadata
    timestamp = datetime.now()
    template = metadata.read_template(dataset, version=version)
    version = version or metadata.get_version(template, timestamp)

    # get file
    file = download_file_from_source(template, version)

    # create "final" config file
    config = metadata.get_config(template, version, timestamp, file.name)
    # archive to edm-recipes/raw_datasets
    recipes.archive_raw_dataset(config, file)

    return config


def transform_to_parquet(config: Config, local_data_path: Path | None = None):
    """
    Transforms raw data into a parquet file format and saves it locally.

    This function first checks for the presence of raw data locally at the specified `local_data_path`.
    If the path not provided, it is downloaded from an S3 bucket using the `config` parameter.
    The raw data is then read into a GeoDataFrame and saved as a parquet file.

    The transformation process varies depending on the format of the raw data, which can be in .shp, .gdb,
    or .csv format. For csv files, if geometry is present, it is converted into a GeoSeries before creating
    the GeoDataFrame.

    Parameters:
        config (recipes.ExtractConfig): Config object containing geometry info.
        local_data_path (Path, optional): Path to the local data file. If not provided, data is pulled from S3 bucket.

    Raises:
        AssertionError: If `local_data_path` is provided but does not point to a valid file or directory.
        AssertionError: If `geom_column` is present in yaml template but not in the dataset.
    """

    # create new dir for raw data and output parquet file
    if TMP_DIR.is_dir():
        shutil.rmtree(TMP_DIR)
    TMP_DIR.mkdir()

    if local_data_path:
        assert (
            local_data_path.is_file() or local_data_path.is_dir()
        ), "Local path should be a valid file or directory"
        logger.info(f"✅ Raw data was found locally at {local_data_path}")
    else:
        local_data_path = TMP_DIR / config.raw_filename

        s3.download_file(
            bucket=recipes.BUCKET,
            key=str(config.raw_dataset_s3_filepath),
            path=local_data_path,
        )
        logger.info(f"Downloaded raw data from s3 to {local_data_path}")

    data_load_config = config.file_format

    # TODO: rename geom column to "geom" regardless of input data type
    match data_load_config:
        case file.Shapefile() as shapefile:
            gdf = gpd.read_file(
                local_data_path,
                crs=shapefile.crs,
                encoding=shapefile.encoding,
            )
        case file.Geodatabase() as geodatabase:
            gdf = gpd.read_file(
                local_data_path,
                crs=geodatabase.crs,
                encoding=geodatabase.encoding,
                layer=geodatabase.layer,
            )
        case file.Csv() as csv:
            df = pd.read_csv(
                local_data_path,
                index_col=False,
                encoding=data_load_config.encoding,
                delimiter=data_load_config.delimiter,
            )

            if not csv.geometry:
                gdf = df

            else:
                # case when geometry is in one column (i.e. polygon or point object type)
                if isinstance(csv.geometry.geom_column, str):
                    geom_column = csv.geometry.geom_column
                    assert (
                        geom_column in df.columns
                    ), f"❌ Geometry column specified in recipe template does not exist in {config.raw_filename}"

                    # replace NaN values with None. Otherwise gpd throws an error
                    if df[geom_column].isnull().any():
                        df[geom_column] = df[geom_column].astype(object)
                        df[geom_column] = df[geom_column].where(
                            df[geom_column].notnull(), None
                        )

                    df[geom_column] = gpd.GeoSeries.from_wkt(df[geom_column])

                    gdf = gpd.GeoDataFrame(
                        df,
                        geometry=geom_column,
                        crs=csv.geometry.crs,
                    )

    gdf.to_parquet(PARQUET_PATH, index=False)
    logger.info(f"✅ Converted raw data to parquet file and saved as {PARQUET_PATH}")


def validate_dataset(config: Config):
    """Given config of imported dataset, validate data expectations/contract"""
    raise NotImplemented
