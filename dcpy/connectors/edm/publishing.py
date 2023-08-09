from pathlib import Path
import requests
import pandas as pd

from dcpy.utils import s3

BUCKET = "edm-publishing"
BASE_URL = f"https://{BUCKET}.nyc3.digitaloceanspaces.com"


## TODO - these functions need some cleaning
## ideally they don't need to called individually, but we either have consistent folder structure
## Or have metadata in json/etc about structure of outputs per dataset
def get_dataset_branch_path(dataset: str, branch: str, version: str):
    return f"{BASE_URL}/{dataset}/{branch}/{version}/output"


def get_dataset_path(dataset: str, version: str):
    return f"{BASE_URL}/{dataset}/{version}/output"


def get_latest_version(dataset: str):
    """Given dataset name, gets latest version
    Assumes that dataset follows standard folder structure of {dataset}/{version}/output/version.txt
    """

    version = requests.get(
        f"{get_dataset_path(dataset=dataset, version='latest')}/version.txt",
        timeout=10,
    ).text
    return version


def get_source_data_versions(
    dataset: str, version: str = "latest", branch=None
) -> pd.DataFrame:
    """Given dataset name, gets source data versions
    Assumes that dataset follows standard folder structure of {dataset}/{version}/output/
    """
    if branch is None:
        output_url = get_dataset_path(dataset=dataset, version=version)
    else:
        output_url = get_dataset_branch_path(
            dataset=dataset, branch=branch, version=version
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
    output: Path,
    publishing_folder: str,
    version: str,
    acl: str,
    *,
    s3_subpath: str = None,
    latest: bool = True,
    include_foldername: bool = False,
    max_files: int = 20,
):
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
            s3.copy_folder(BUCKET, version_folder, prefix / "latest", acl, max_files)
    else:
        s3.upload_file("edm-publishing", output, str(key), "public-read")
        if latest:
            ## much faster than uploading again
            s3.copy_file(BUCKET, str(key), str(prefix / "latest" / output.name), acl)
