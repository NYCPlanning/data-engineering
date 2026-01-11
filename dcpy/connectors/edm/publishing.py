import geopandas as gpd
from io import BytesIO
import pandas as pd
from pathlib import Path
import typer
from typing import Callable, TypeVar
from zipfile import ZipFile

from dcpy.configuration import (
    PUBLISHING_BUCKET,
)
from dcpy.connectors.edm.drafts import get_draft_version_revisions
from dcpy.models.connectors.edm.publishing import (
    ProductKey,
    DraftKey,
    BuildKey,
)
from dcpy.utils import s3
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.recipes connector"
    )
    return PUBLISHING_BUCKET


def get_file(product_key: ProductKey, filepath: str) -> BytesIO:
    """Returns file as BytesIO given product key and path within output folder"""
    return s3.get_file_as_stream(_bucket(), f"{product_key.path}/{filepath}")


T = TypeVar("T")


def _read_data_helper(path: str, filereader: Callable[[BytesIO], T], **kwargs) -> T:
    bucket = _bucket()
    if not s3.object_exists(bucket, path):
        raise FileNotFoundError(f"publishing file {path} not found.")
    with s3.get_file_as_stream(bucket, path) as stream:
        data = filereader(stream, **kwargs)
    return data


def read_csv(product_key: ProductKey, filepath: str, **kwargs) -> pd.DataFrame:
    """Reads csv into pandas dataframe from edm-publishing
    Works for zipped or standard csvs

    Keyword arguments:
    product_key -- a key to find a specific instance of a data product
    filepath -- the filepath of the desired csv in the output folder
    """
    return _read_data_helper(f"{product_key.path}/{filepath}", pd.read_csv, **kwargs)


def read_csv_legacy(
    product: str, version: str, filepath: str, **kwargs
) -> pd.DataFrame:
    """Legacy function which reads csv into pandas dataframe from edm-publishing
    Works for zipped or standard csvs
    Replaced by read_csv which assumes outputs are in "publish" or "draft" folders in s3

    Keyword arguments:
    product -- the name of the data product
    version -- a specific version of a product
    filepath -- the filepath of the desired csv in the output folder
    """
    with s3.get_file_as_stream(_bucket(), f"{product}/{version}/{filepath}") as stream:
        df = pd.read_csv(stream, **kwargs)
    return df


def read_shapefile(product_key: ProductKey, filepath: str) -> gpd.GeoDataFrame:
    """Reads published shapefile into geopandas dataframe from edm-publishing

    Keyword arguments:
    product_key -- a key to find a specific instance of a data product
    filepath -- the filepath of the desired csv in the output folder
    """
    return _read_data_helper(f"{product_key.path}/{filepath}", gpd.read_file)


def get_zip(product_key: ProductKey, filepath: str) -> ZipFile:
    """Reads zip file into ZipFile object from edm-publishing

    Keyword arguments:
    product_key -- a key to find a specific instance of a data product
    filepath -- the filepath of the desired csv in the output folder
    """
    stream = s3.get_file_as_stream(_bucket(), f"{product_key.path}/{filepath}")
    zip = ZipFile(stream)
    return zip


app = typer.Typer(add_completion=False)


@app.command("upload")
def _cli_wrapper_upload(
    output_path: Path = typer.Option(
        Path("output"), "-o", "--output-path", help="Path to local output folder"
    ),
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Name of data product (publishing folder in s3)",
    ),
    build: str = typer.Option(None, "-b", "--build", help="Label of build"),
    acl: str = typer.Option(None, "-a", "--acl", help="Access level of file in s3"),
    max_files: int = typer.Option(
        s3.MAX_FILE_COUNT, "--max_files", help="Max number of files to upload"
    ),
):
    acl_literal = s3.string_as_acl(acl)
    upload_build(
        output_path, product, acl=acl_literal, build=build, max_files=max_files
    )


@app.command("publish")
def _cli_wrapper_publish(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Data product name (folder name in S3 publishing bucket)",
    ),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Data product release version (draft version folder name in s3)",
    ),
    draft_revision_num: int = typer.Option(
        None,
        "-dn",
        "--draft-number",
        help="Draft revision number to publish. If blank, will use latest draft",
    ),
    acl: str = typer.Option(None, "-a", "--acl", help="Access level of file in s3"),
    latest: bool = typer.Option(
        False, "-l", "--latest", help="Publish to latest folder as well?"
    ),
    is_patch: bool = typer.Option(
        False,
        "-ip",
        "--is-patch",
        help="Create a patched version if version already exists?",
    ),
    download_metadata: bool = typer.Option(
        False,
        "-m",
        "--download-metadata",
        help="Download metadata from 'publish' folder after publishing?",
    ),
):
    acl_literal = s3.string_as_acl(acl)

    # Get draft_key
    if draft_revision_num is not None:
        draft_revision_label = get_draft_revision_label(
            product, version, draft_revision_num
        )
    else:
        draft_revision_label = get_draft_version_revisions(product, version)[0]
    draft_key = DraftKey(
        product=product, version=version, revision=draft_revision_label
    )
    publish(
        draft_key=draft_key,
        acl=acl_literal,
        latest=latest,
        is_patch=is_patch,
        download_metadata=download_metadata,
    )


@app.command("validate_or_patch_version")
def _cli_wrapper_validate_or_patch_version(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Data product name (folder name in S3 publishing bucket)",
    ),
    version: str = typer.Option(None, "-v", "--version", help="Product version"),
    is_patch: bool = typer.Option(
        False,
        "-ip",
        "--is-patch",
        help="Is this a patch for an existing version?",
    ),
):
    print(
        validate_or_patch_version(product=product, version=version, is_patch=is_patch)
    )


@app.command("promote_to_draft")
def _cli_wrapper_promote_to_draft(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Data product name (folder name in S3 publishing bucket)",
    ),
    build: str = typer.Option(None, "-b", "--build", help="Label of build"),
    draft_summary: str = typer.Option(
        "",
        "-ds",
        "--draft-summary",
        help="Draft description (becomes a part of draft name in s3)",
    ),
    acl: str = typer.Option(None, "-a", "--acl", help="Access level of file in s3"),
    download_metadata: bool = typer.Option(
        False,
        "-m",
        "--download-metadata",
        help="Download metadata from 'draft' folder after promoting to drafts?",
    ),
):
    acl_literal = s3.string_as_acl(acl)
    build_key = BuildKey(product, build)
    logger.info(f'Promoting {build_key.path} to draft with ACL "{acl}"')
    promote_to_draft(
        build_key=build_key,
        draft_revision_summary=draft_summary,
        acl=acl_literal,
        download_metadata=download_metadata,
    )


if __name__ == "__main__":
    app()
