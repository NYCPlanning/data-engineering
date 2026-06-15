from urllib.parse import urlencode, urljoin
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.connectors.edm.models import BuildKey, DraftKey, ProductKey, PublishKey
from dcpy.lifecycle.builds import builds, drafts, published
from dcpy.utils import s3


def unzip_csv(csv_filename: str, zipfile: ZipFile) -> pd.DataFrame:
    with zipfile.open(csv_filename) as csv:
        return pd.read_csv(csv, true_values=["t"], false_values=["f"])


def read_file_metadata(product_key: ProductKey, filepath: str) -> s3.Metadata:
    assert PUBLISHING_BUCKET, "PUBLISHING_BUCKET must be defined"
    return s3.get_metadata(PUBLISHING_BUCKET, f"{product_key.path}/{filepath}")


def read_csv_cached(product_key: ProductKey, filepath: str, **kwargs) -> pd.DataFrame:
    """Read CSV from any lifecycle stage (builds, drafts, or published).

    Note: Caching removed from this wrapper to support unhashable kwargs like 'converters'.
    Caching should be done at the component level if needed.
    """
    if isinstance(product_key, BuildKey):
        return builds.read_csv(
            product_key.product, product_key.build, filepath, **kwargs
        )
    elif isinstance(product_key, DraftKey):
        return drafts.read_csv(
            product_key.product,
            product_key.version,
            product_key.revision,
            filepath,
            **kwargs,
        )
    elif isinstance(product_key, PublishKey):
        return published.read_csv(
            product_key.product, product_key.version, filepath, **kwargs
        )
    else:
        raise TypeError(f"Unsupported product key type: {type(product_key)}")


def read_shapefile_cached(product_key: ProductKey, filepath: str) -> gpd.GeoDataFrame:
    """Read shapefile from any lifecycle stage (builds, drafts, or published).

    Note: Caching removed - GeoDataFrames should be cached at component level if needed.
    """
    if isinstance(product_key, BuildKey):
        return builds.read_shapefile(product_key.product, product_key.build, filepath)
    elif isinstance(product_key, DraftKey):
        return drafts.read_shapefile(
            product_key.product, product_key.version, product_key.revision, filepath
        )
    elif isinstance(product_key, PublishKey):
        return published.read_shapefile(
            product_key.product, product_key.version, filepath
        )
    else:
        raise TypeError(f"Unsupported product key type: {type(product_key)}")


def get_zip_cached(product_key: ProductKey, filepath: str) -> ZipFile:
    """Get ZipFile from any lifecycle stage (builds, drafts, or published).

    Note: Caching removed - ZipFile objects are not serializable by st.cache_data.
    If caching is needed, extract the data first and cache that at component level.
    """
    if isinstance(product_key, BuildKey):
        return builds.get_zip(product_key.product, product_key.build, filepath)
    elif isinstance(product_key, DraftKey):
        return drafts.get_zip(
            product_key.product, product_key.version, product_key.revision, filepath
        )
    elif isinstance(product_key, PublishKey):
        return published.get_zip(product_key.product, product_key.version, filepath)
    else:
        raise TypeError(f"Unsupported product key type: {type(product_key)}")


def get_data_directory_url(product_key: ProductKey) -> str:
    """Get Digital Ocean data directory URL for any lifecycle stage."""
    assert PUBLISHING_BUCKET, "PUBLISHING_BUCKET must be defined"
    path = product_key.path
    if not path.endswith("/"):
        path += "/"
    endpoint = urlencode({"path": path})
    url = urljoin(
        f"https://cloud.digitalocean.com/spaces/{PUBLISHING_BUCKET}", "?" + endpoint
    )
    return url
