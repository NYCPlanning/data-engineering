import os
from pathlib import Path
from urllib.parse import urlencode, urljoin
import pandas as pd
import geopandas as gpd
from datetime import datetime
import pytz
import re
from io import BytesIO
from zipfile import ZipFile
import typer
from typing import Callable, TypeVar
import yaml
import json

from dcpy.utils import s3, git, versions
from dcpy.utils.logging import logger

from dcpy.models.connectors.edm.publishing import (
    ProductKey,
    PublishKey,
    DraftKey,
)
from dcpy.models.lifecycle.builds import BuildMetadata

from botocore.exceptions import ClientError

BUCKET = "edm-publishing"
BASE_DO_URL = f"https://cloud.digitalocean.com/spaces/{BUCKET}"


def get_build_metadata(product_key: ProductKey) -> BuildMetadata:
    """Retrieve a product build metadata from s3."""
    obj = s3.client().get_object(
        Bucket=BUCKET, Key=f"{product_key.path}/build_metadata.json"
    )
    file_content = str(obj["Body"].read(), "utf-8")
    return BuildMetadata(**yaml.safe_load(file_content))


def get_version(product_key: ProductKey) -> str | None:
    """
    Given product key, gets version. None is returned when metadata file
    for given product_key doesn't exist.
    """
    try:
        return get_build_metadata(product_key).version
    except ClientError:
        return None


def get_latest_version(product: str) -> str | None:
    """Given product name, gets latest version
    Assumes existence of build_metadata.json in output folder
    """
    return get_version(PublishKey(product, "latest"))


def get_published_versions(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/publish/"), reverse=True)


def get_draft_builds(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/draft/"), reverse=True)


def get_previous_version(
    product: str, version: str | versions.Version
) -> versions.Version:
    match version:
        case str():
            version_obj = versions.parse(version)
        case versions.Version():
            version_obj = version

    published_version_strings = get_published_versions(product)
    logger.info(f"Published versions of {product}: {published_version_strings}")
    published_versions = [
        versions.parse(v) for v in published_version_strings if v != "latest"
    ]
    published_versions.sort()
    if len(published_versions) == 0:
        raise LookupError(f"No published versions found for {product}")
    if version_obj in published_versions:
        index = published_versions.index(version_obj)
        if index == 0:
            raise LookupError(
                f"{product} - {version} is the oldest published version, and has no previous"
            )
        else:
            return published_versions[published_versions.index(version_obj) - 1]
    else:
        latest = published_versions[-1]
        if version_obj > latest:
            return latest
        else:
            raise LookupError(
                f"{product} - {version} is not published and appears to be 'older' than latest published version. Cannot determine previous."
            )


def get_source_data_versions(product_key: ProductKey) -> pd.DataFrame:
    """Given product name, gets source data versions of published version"""
    source_data_versions = read_csv(product_key, "source_data_versions.csv", dtype=str)
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


def generate_metadata() -> dict[str, str]:
    """Generates "standard" s3 metadata for our files"""
    metadata = {
        "date_created": datetime.now(pytz.timezone("America/New_York")).isoformat()
    }
    metadata["commit"] = git.commit_hash()
    if os.environ.get("CI"):
        metadata["run_url"] = git.action_url()
    return metadata


def upload(
    output_path: Path,
    draft_key: DraftKey,
    *,
    acl: s3.ACL,
    max_files: int = s3.MAX_FILE_COUNT,
) -> None:
    """Upload build output(s) to draft folder in edm-publishing"""
    draft_path = draft_key.path
    meta = generate_metadata()
    if output_path.is_dir():
        s3.upload_folder(
            BUCKET,
            output_path,
            Path(draft_path),
            acl,
            contents_only=True,
            max_files=max_files,
            metadata=meta,
        )
    else:
        s3.upload_file(
            "edm-publishing",
            output_path,
            f"{draft_path}/{output_path.name}",
            acl,
            metadata=meta,
        )


def legacy_upload(
    output: Path,
    publishing_folder: str,
    version: str,
    acl: s3.ACL,
    *,
    s3_subpath: str | None = None,
    latest: bool = True,
    contents_only: bool = False,
    max_files: int = s3.MAX_FILE_COUNT,
) -> None:
    """Upload file or folder to publishing, with more flexibility around s3 subpath
    Currently used only by db-factfinder"""
    if s3_subpath is None:
        prefix = Path(publishing_folder)
    else:
        prefix = Path(publishing_folder) / s3_subpath
    key = prefix / version
    meta = generate_metadata()
    if output.is_dir():
        s3.upload_folder(
            BUCKET,
            output,
            key,
            acl,
            contents_only=contents_only,
            max_files=max_files,
            metadata=meta,
        )
        if latest:
            ## much faster than uploading again
            s3.copy_folder(
                BUCKET,
                str(key) + "/",
                str(prefix / "latest"),
                acl,
                max_files=max_files,
            )
    else:
        s3.upload_file("edm-publishing", output, str(key), "public-read", metadata=meta)
        if latest:
            ## much faster than uploading again
            s3.copy_file(BUCKET, str(key), str(prefix / "latest" / output.name), acl)


def publish(
    draft_key: DraftKey,
    *,
    acl: s3.ACL,
    version: str | None = None,
    keep_draft: bool = True,
    max_files: int = s3.MAX_FILE_COUNT,
    latest: bool = False,
    is_patch: bool = False,
) -> None:
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    if version is None:
        version = get_version(draft_key)
        assert version is not None

    version_already_published = version in get_published_versions(
        product=draft_key.product
    )
    # Bump version if input version already exists. Update metadata
    if version_already_published and is_patch:
        build_metadata = get_build_metadata(
            product_key=PublishKey(draft_key.product, version)
        )
        version = versions.bump(
            previous_version=version, bump_type=versions.VersionSubType.patch, bump_by=1
        ).label

        # update version in metadata and dump it locally
        build_metadata.version = version
        build_metadata_path = Path("build_metadata.json")
        with open(build_metadata_path, "w", encoding="utf-8") as f:
            json.dump(build_metadata.model_dump(), f, indent=4)
    elif version_already_published and not is_patch:
        raise ValueError(
            f"Version '{version}' already exists. Enter a different version value or select to patch a version."
        )
    else:
        build_metadata = None

    source = draft_key.path + "/"
    target = f"{draft_key.product}/publish/{version}/"
    s3.copy_folder(BUCKET, source, target, acl, max_files=max_files)

    # upload metadata if a version was patched
    if build_metadata:
        s3.upload_file(
            BUCKET,
            build_metadata_path,
            f"{target}{build_metadata_path.name}",
            acl,
        )
    # if current version comes after 'latest' version or there are no files in 'latest' folder,
    # update 'latest' folder
    if latest:
        latest_version = get_latest_version(draft_key.product)
        if latest_version:
            # Both latest_version and version are expected to be of same version schema
            after_latest_version = versions.is_newer(
                version_1=version, version_2=latest_version
            )
        else:
            after_latest_version = None

        if after_latest_version or latest_version is None:
            s3.copy_folder(
                BUCKET,
                source,
                f"{draft_key.product}/publish/latest/",
                acl,
                max_files=max_files,
            )
            if build_metadata:
                s3.upload_file(
                    BUCKET,
                    build_metadata_path,
                    f"{draft_key.product}/publish/latest/{build_metadata_path.name}",
                    acl,
                )
        else:
            raise ValueError(
                f"Unable to update 'latest' folder: the version {version} is older than 'latest' ({latest_version})"
            )

    if not keep_draft:
        s3.delete(BUCKET, source)


def download_published_version(
    publish_key: PublishKey,
    output_dir: Path | None = None,
    *,
    include_prefix_in_export: bool = True,
) -> None:
    output_dir = output_dir or Path(".")
    published_versions = get_published_versions(product=publish_key.product)
    assert (
        publish_key.version in published_versions
    ), f"{publish_key} not found in S3 bucket '{BUCKET}'. Published versions are {published_versions}"
    s3.download_folder(
        BUCKET,
        f"{publish_key.path}/",
        output_dir,
        include_prefix_in_export=include_prefix_in_export,
    )


def file_exists(product_key: ProductKey, filepath: str) -> bool:
    """Returns true if given file exists within outputs for given product key"""
    return s3.exists(bucket=BUCKET, key=f"{product_key.path}/{filepath}")


def get_file(product_key: ProductKey, filepath: str) -> BytesIO:
    """Returns file as BytesIO given product key and path within output folder"""
    return s3.get_file_as_stream(BUCKET, f"{product_key.path}/{filepath}")


T = TypeVar("T")


def _read_data_helper(path: str, filereader: Callable[[BytesIO], T], **kwargs) -> T:
    with s3.get_file_as_stream(BUCKET, path) as stream:
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
    with s3.get_file_as_stream(BUCKET, f"{product}/{version}/{filepath}") as stream:
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
    stream = s3.get_file_as_stream(BUCKET, f"{product_key.path}/{filepath}")
    zip = ZipFile(stream)
    return zip


def download_file(
    product_key: ProductKey, filepath: str, output_dir: Path | None = None
) -> Path:
    output_dir = output_dir or Path(".")
    output_filepath = output_dir / Path(filepath).name
    s3.download_file(BUCKET, f"{product_key.path}/{filepath}", output_filepath)
    return output_filepath


def publish_add_created_date(
    product: str,
    source: str,
    acl: s3.ACL,
    *,
    version: str | None = None,
    file_for_creation_date: str = "version.txt",
    max_files: int = s3.MAX_FILE_COUNT,
) -> dict[str, str]:
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    if version is None:
        with s3.get_file(BUCKET, f"{source}version.txt") as f:
            version = str(f.read())
    print(version)
    old_metadata = s3.get_metadata(BUCKET, f"{source}{file_for_creation_date}")
    target = f"{product}/publish/{version}/"
    s3.copy_folder(
        BUCKET,
        source,
        target,
        acl,
        max_files=max_files,
        metadata={"date_created": old_metadata.last_modified.isoformat()},
    )
    return {"date_created": old_metadata.last_modified.isoformat()}


def get_data_directory_url(product_key: ProductKey) -> str:
    """Returns url of the data directory in Digital Ocean."""

    path = product_key.path
    if not path.endswith("/"):
        path += "/"
    endpoint = urlencode({"path": path})
    url = urljoin(BASE_DO_URL, "?" + endpoint)

    return url


def _gis_dataset_path(name: str, version: str) -> str:
    return f"datasets/{name}/{version}/{name}.zip"


def _assert_gis_dataset_exists(name: str, version: str):
    version = version.upper()
    if not s3.exists(BUCKET, _gis_dataset_path(name, version)):
        print(_gis_dataset_path(name, version))
        print(s3.list_objects(BUCKET, _gis_dataset_path(name, version)))
        print(s3.exists(BUCKET, _gis_dataset_path(name, version)))
        raise FileNotFoundError(f"GIS dataset {name} has no version {version}")


def get_latest_gis_dataset_version(dataset_name: str) -> str:
    """
    Get latest version of GIS-published dataset in edm-publishing/datasets
    assuming versions are sortable
    """
    gis_version_formats = [r"^\d{2}[A-Z]$", r"^\d{8}$"]
    subfolders = []
    matched_formats = set()
    for f in s3.get_subfolders(BUCKET, f"datasets/{dataset_name}"):
        for p in gis_version_formats:
            if re.match(p, f):
                subfolders.append(f)
                matched_formats.add(p)
    if subfolders:
        if len(matched_formats) > 1:
            raise ValueError(
                f"Multiple version formats found for gis dataset {dataset_name}. Cannot determine latest version"
            )
    version = max(subfolders)
    _assert_gis_dataset_exists(dataset_name, version)
    return version


def download_gis_dataset(dataset_name: str, version: str, target_folder: Path):
    """
    Download GIS-published dataset from edm-publishing/datasets.
    Capitalizes supplied version when looking in s3 due to current conventions.
    Only quarterly (24a/24A) datasets currently use format other than just numeric datestrings
    """
    ## TODO - assumes versions are numeric or geosupport (which we use "24a" vs gis "24A")
    version = version.upper()
    assert target_folder.is_dir(), f"Target folder '{target_folder}' is not a directory"
    _assert_gis_dataset_exists(dataset_name, version)
    file_path = target_folder / f"{dataset_name}.zip"  ## we assume all gis datasets are
    s3.download_file(BUCKET, _gis_dataset_path(dataset_name, version), file_path)
    return file_path


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
):
    acl_literal = s3.string_as_acl(acl)
    if not output_path.exists():
        raise FileNotFoundError(f"Path {output_path} does not exist")
    build_name = build or os.environ["BUILD_NAME"]
    if not build_name:
        raise ValueError(
            f"Build name supplied via CLI or the env var 'BUILD_NAME' cannot be '{build_name}'."
        )
    logger.info(
        f'Uploading {output_path} to {product}/draft/{build_name} with ACL "{acl}"'
    )
    upload(output_path, DraftKey(product, build_name), acl=acl_literal)


@app.command("publish")
def _cli_wrapper_publish(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Data product name (folder name in S3 publishing bucket)",
    ),
    build: str = typer.Option(None, "-b", "--build", help="Label of build"),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Data product release version",
    ),
    acl: str = typer.Option(None, "-a", "--acl", help="Access level of file in s3"),
    latest: bool = typer.Option(
        False, "-l", "--latest", help="Publish to latest folder as well?"
    ),
):
    acl_literal = s3.string_as_acl(acl)
    logger.info(
        f'Publishing {product}/draft/{build} as version {version} with ACL "{acl}"'
    )
    publish(
        DraftKey(product, build),
        acl=acl_literal,
        version=version,
        latest=latest,
    )


@app.command("download_file")
def _cli_wrapper_download_file(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Name of data product (publishing folder in s3)",
    ),
    draft: bool = typer.Option(
        False,
        "-d",
        "--draft",
    ),
    version: str = typer.Option(None, "-v", "--version", help="Product version"),
    filepath: str = typer.Option(
        None, "-f", "--filepath", help="Filepath within s3 output folder"
    ),
    output_dir: Path = typer.Option(
        None, "-o", "--output-dir", help="Folder to download file to"
    ),
):
    if draft:
        key: ProductKey = DraftKey(product=product, build=version)
    else:
        key = PublishKey(product=product, version=version)
    download_file(key, filepath, output_dir)


if __name__ == "__main__":
    app()
