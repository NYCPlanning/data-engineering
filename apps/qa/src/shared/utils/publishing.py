from zipfile import ZipFile

import pandas as pd
import streamlit as st

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


@st.cache_data(ttl=600)
def read_csv_cached(product_key: ProductKey, filepath: str, **kwargs) -> pd.DataFrame:
    """Read CSV from any lifecycle stage (builds, drafts, or published)."""
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
