from pathlib import Path
import requests
from zipfile import ZipFile
import pandas as pd
from typing import Optional
import geopandas as gpd

from dcpy.utils import s3

BUCKET = "edm-publishing"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com"


## TODO - these functions need some cleaning
## ideally they don't need to called individually, but we either have consistent folder structure
## Or have metadata in json/etc about structure of outputs per dataset
def get_dataset_path(dataset: str, *, branch: str, version="latest", published=False):
    """Construct s3 path for a published dataset."""
    return (
        (
            f"{BASE_URL}/{dataset}/"
            + (branch + "/" if branch is not None else "")
            + version
            + "/output"
        )
        if not published
        else (f"{BASE_URL}/{dataset}/" + "publish/" + version)
    )


def get_latest_version(dataset: str, *, branch: str = "", published=False):
    """Given dataset name, gets latest version
    Assumes that dataset follows standard folder structure of {dataset}/{version}/output/version.txt
    """
    path = f"{get_dataset_path(dataset=dataset,version='latest', branch=branch, published=published)}/version.txt"
    print(f"finding latest version from {path}")
    return requests.get(
        path,
        timeout=10,
    ).text.splitlines()[0]


def get_source_data_versions(
    dataset: str, version: str = "latest", branch=None, published=False
) -> pd.DataFrame:
    """Given dataset name, gets source data versions
    Assumes that dataset follows standard folder structure of {dataset}/{version}/output/
    """
    output_url = get_dataset_path(
        dataset=dataset, branch=branch, version=version, published=published
    )

    source_data_versions = pd.read_csv(
        f"{output_url}/source_data_versions.csv",
        index_col=False,
        dtype=str,
    )
    source_data_versions.rename(
        columns={
            "schema_name": "datalibrary_name",
            "v": "version",
        },
        errors="raise",
        inplace=True,
    )
    source_data_versions.sort_values(
        by=["datalibrary_name"], ascending=True
    ).reset_index(drop=True, inplace=True)
    source_data_versions.set_index("datalibrary_name", inplace=True)
    return source_data_versions


def upload(
    output_path: Path,
    product: str,
    build: str,
    acl: str,
    *,
    max_files: int = 20,
):
    """Upload build output(s) to draft folder in edm-publishing"""
    folder_key = f"{product}/draft/{build}"
    if output_path.is_dir():
        s3.upload_folder(
            BUCKET,
            output_path,
            Path(folder_key),
            acl,
            include_foldername=False,
            max_files=max_files,
        )
    else:
        s3.upload_file(
            "edm-publishing", output_path, f"{folder_key}/{output_path.name}", acl
        )


def legacy_upload(
    output: Path,
    publishing_folder: str,
    version: str,
    acl: str,
    *,
    s3_subpath: Optional[str] = None,
    latest: bool = True,
    include_foldername: bool = False,
    max_files: int = 20,
):
    """Upload file or folder to publishing, with more flexibility around s3 subpath
    Currently used only by db-factfinder"""
    if s3_subpath is None:
        prefix = Path(publishing_folder)
    else:
        prefix = Path(publishing_folder) / s3_subpath
    version_folder = prefix / version
    key = version_folder / output.name
    if output.is_dir():
        s3.upload_folder(
            BUCKET,
            output,
            key,
            acl,
            include_foldername=include_foldername,
            max_files=max_files,
        )
        if latest:
            ## much faster than uploading again
            s3.copy_folder(
                BUCKET,
                str(version_folder),
                str(prefix / "latest"),
                acl,
                max_files=max_files,
            )
    else:
        s3.upload_file("edm-publishing", output, str(key), "public-read")
        if latest:
            ## much faster than uploading again
            s3.copy_file(BUCKET, str(key), str(prefix / "latest" / output.name), acl)


def publish(
    product: str,
    build: str,
    publishing_version: str,  ## TODO - ideally this is optional, and we have mechanisms for determining programmatically
    acl: str,
    *,
    keep_draft: bool = True,
    max_files: int = 30,
):
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    source = f"{product}/draft/{build}/"
    target = f"{product}/publish/{publishing_version}/"
    s3.copy_folder(BUCKET, source, target, acl, max_files=max_files)
    if not keep_draft:
        s3.delete(BUCKET, source)


def read_csv(dataset, version, filepath, **kwargs):
    """Reads csv into pandas dataframe from edm-publishing
    'version' currently must be a path from root dataset folder in edm-publishing to output folder
    Works for zipped or standard csvs
    """
    print(f"{dataset}/{version}/{filepath}")
    with s3.get_file_as_stream(BUCKET, f"{dataset}/{version}/{filepath}") as stream:
        df = pd.read_csv(stream, **kwargs)
    return df


def read_shapefile(dataset, version, filepath):
    with s3.get_file_as_stream(BUCKET, f"{dataset}/{version}/{filepath}") as stream:
        gdf = gpd.read_file(stream)
    return gdf


def get_zip(dataset, version, filepath):
    stream = s3.get_file_as_stream(BUCKET, f"{dataset}/{version}/{filepath}")
    zip = ZipFile(stream)
    return zip
