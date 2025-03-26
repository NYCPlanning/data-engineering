from pathlib import Path
import shutil

from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    S3Source,
    ScriptSource,
    DEPublished,
    ESRIFeatureServer,
    Source,
)
from dcpy.models.connectors import socrata, web as web_models
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.utils.logging import logger
from dcpy.utils import s3
from dcpy.connectors.edm import publishing
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors.esri import arcgis_feature_service
from dcpy.connectors import web


def download_file_from_source(
    source: Source, filename: str, version: str, dir: Path
) -> None:
    """
    From parsed config template and version, download raw data from source to provided path
    """
    path = dir / filename
    logger.info(f"Extracting {path.name} from source to staging folder")
    match source:
        ## Non reqeust-based methods
        case LocalFileSource():
            if source.path != path:
                shutil.copy(source.path, path)
        case GisDataset():
            publishing.download_gis_dataset(source.name, version, dir)
        case DEPublished():
            publishing.download_file(
                publishing.PublishKey(product=source.product, version=version),
                filepath=source.filename,
                output_dir=dir,
            )
        case S3Source():
            s3.download_file(source.bucket, source.key, path)
        case ScriptSource():
            raise NotImplementedError("Custom scripts not yet supported in ingest.")

        ## request-based methods
        case web_models.FileDownloadSource():
            web.download_file(source.url, path)
        case web_models.GenericApiSource():
            web.download_file(source.endpoint, path)
        case socrata.Source():
            extract_socrata.download(source, path)
        case ESRIFeatureServer():
            arcgis_feature_service.download_layer(
                source.feature_server_layer,
                source.crs,
                path,
            )
        case _:
            raise NotImplementedError(
                f"Source type {source.type} not supported for download_file_from_source"
            )
