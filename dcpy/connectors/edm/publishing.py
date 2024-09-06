from botocore.exceptions import ClientError
from datetime import datetime
import geopandas as gpd
from io import BytesIO
import json
import pandas as pd
from pathlib import Path
import pytz
import re
import typer
from typing import Callable, TypeVar
from urllib.parse import urlencode, urljoin
import yaml
from zipfile import ZipFile

from dcpy.configuration import PUBLISHING_BUCKET, CI, BUILD_NAME
from dcpy.models.connectors.edm.publishing import (
    ProductKey,
    PublishKey,
    DraftKey,
    BuildKey,
)
from dcpy.models.lifecycle.builds import BuildMetadata
from dcpy.utils import s3, git, versions
from dcpy.utils.logging import logger

assert (
    PUBLISHING_BUCKET
), "'PUBLISHING_BUCKET' must be defined to use edm.publishing connector"
BUCKET = PUBLISHING_BUCKET

BASE_DO_URL = f"https://cloud.digitalocean.com/spaces/{BUCKET}"


def get_build_metadata(product_key: ProductKey) -> BuildMetadata:
    """Retrieve a product build metadata from s3."""
    obj = s3.client().get_object(
        Bucket=BUCKET, Key=f"{product_key.path}/build_metadata.json"
    )
    file_content = str(obj["Body"].read(), "utf-8")
    return BuildMetadata(**yaml.safe_load(file_content))


def get_version(product_key: ProductKey) -> str:
    """Given product key, gets version."""
    return get_build_metadata(product_key).version


def get_latest_version(product: str) -> str | None:
    """Given product name, gets latest version
    Assumes existence of build_metadata.json in output folder
    """
    try:
        return get_version(PublishKey(product, "latest"))
    except ClientError:
        return None


def get_published_versions(product: str, exclude_latest: bool = True) -> list[str]:
    all_versions = s3.get_subfolders(BUCKET, f"{product}/publish/")
    output = (
        [v for v in all_versions if v != "latest"] if exclude_latest else all_versions
    )
    return sorted(output, reverse=True)


def get_draft_versions(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/draft/"), reverse=True)


def get_draft_version_revisions(product: str, version: str) -> list[str]:
    return sorted(
        s3.get_subfolders(BUCKET, f"{product}/draft/{version}/"), reverse=True
    )


def get_draft_revision_label(product: str, version: str, revision_num: int) -> str:
    """Given a draft revision number, return draft revision label in s3."""
    draft_revision_label = None
    draft_revision_objects = [
        versions.parse_draft_version(version)
        for version in get_draft_version_revisions(product, version)
        if versions.parse_draft_version(version).revision_num == revision_num
    ]
    if len(draft_revision_objects) != 0:
        draft_revision_label = draft_revision_objects[0].label
    if draft_revision_label is None:
        raise ValueError(
            f"A draft revision with revision number of {revision_num} doesn't exist. Try again"
        )
    return draft_revision_label


def get_builds(product: str) -> list[str]:
    return sorted(s3.get_subfolders(BUCKET, f"{product}/build/"), reverse=True)


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
    published_versions = [versions.parse(v) for v in published_version_strings]
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


def get_filenames(product_key: ProductKey) -> set[str]:
    return s3.get_filenames(BUCKET, product_key.path)


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
    if CI:
        metadata["run_url"] = git.action_url()
    return metadata


def upload(
    output_path: Path,
    build_key: BuildKey,
    *,
    acl: s3.ACL,
    max_files: int = s3.MAX_FILE_COUNT,
) -> None:
    """Upload build output(s) to build folder in edm-publishing"""
    meta = generate_metadata()
    if not output_path.is_dir():
        raise Exception("'upload' expects output_path to be a directory, not a file")
    s3.upload_folder(
        BUCKET,
        output_path,
        Path(build_key.path),
        acl,
        contents_only=True,
        max_files=max_files,
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
    """
    Upload file or folder to publishing, with more flexibility around s3 subpath
    Currently used only by db-factfinder
    """
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
        s3.upload_file(BUCKET, output, str(key), "public-read", metadata=meta)
        if latest:
            ## much faster than uploading again
            s3.copy_file(BUCKET, str(key), str(prefix / "latest" / output.name), acl)


def promote_to_draft(
    build_key: BuildKey,
    acl: s3.ACL,
    keep_build: bool = True,
    max_files: int = s3.MAX_FILE_COUNT,
    draft_revision_summary: str = "",
):
    version = get_version(build_key)

    # generate version draft revision number
    draft_revision_number = (
        len(get_draft_version_revisions(build_key.product, version)) + 1
    )
    draft_revision_label = versions.DraftVersionRevision(
        draft_revision_number, draft_revision_summary
    ).label

    # read in build metadata, update it with draft label, and save it locally
    build_metadata = get_build_metadata(product_key=build_key)
    build_metadata.draft_revision_name = draft_revision_label
    build_metadata_path = Path("build_metadata.json")
    with open(build_metadata_path, "w", encoding="utf-8") as f:
        json.dump(build_metadata.model_dump(mode="json"), f, indent=4)

    # promote from build to draft
    source = build_key.path + "/"
    target = f"{build_key.product}/draft/{version}/{draft_revision_label}/"
    s3.copy_folder(BUCKET, source, target, acl, max_files=max_files)

    # upload updated metadata file
    s3.upload_file(
        BUCKET,
        build_metadata_path,
        f"{target}{build_metadata_path.name}",
        acl,
    )

    logger.info(
        f"Promoted {build_key.product} to drafts as {version}/{draft_revision_label}"
    )

    if not keep_build:
        s3.delete(BUCKET, source)


def validate_or_patch_version(
    product: str,
    version: str,
    is_patch: bool,
) -> str:
    """Given input arguments, determine the publish version, bumping it if necessary."""
    published_versions = get_published_versions(product=product)

    # Filters existing published versions for same version (patched or non-patched)
    published_same_version = versions.group_versions_by_base(
        version, published_versions
    )
    version_already_published = version in published_same_version

    if version_already_published:
        if is_patch:
            latest_version = published_same_version[-1]
            patched_version = versions.bump(
                previous_version=latest_version,
                bump_type=versions.VersionSubType.patch,
                bump_by=1,
            ).label
            assert patched_version not in published_versions  # sanity check
            return patched_version
        else:
            raise ValueError(
                f"Version '{version}' already exists in published folder and patch wasn't selected"
            )

    logger.info(f"Predicted version in publish folder: {version}")
    return version


def publish(
    draft_key: DraftKey,
    acl: s3.ACL,
    max_files: int = s3.MAX_FILE_COUNT,
    latest: bool = False,
    is_patch: bool = False,
    download_metadata: bool = False,
) -> None:
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    version = get_version(draft_key)
    new_version = validate_or_patch_version(draft_key.product, version, is_patch)

    logger.info(
        f'Publishing {draft_key.path} as version {new_version} with ACL "{acl}"'
    )
    # if it's patch, update version in metadata and dump it locally
    build_metadata = None
    if new_version != version:
        build_metadata = get_build_metadata(product_key=draft_key)
        build_metadata.version = new_version
        build_metadata_path = Path("build_metadata.json")
        with open(build_metadata_path, "w", encoding="utf-8") as f:
            json.dump(build_metadata.model_dump(mode="json"), f, indent=4)

    source = draft_key.path + "/"
    target = f"{draft_key.product}/publish/{new_version}/"
    s3.copy_folder(BUCKET, source, target, acl, max_files=max_files)

    # upload metadata if version was patched
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
            after_latest_version = (
                versions.is_newer(version_1=new_version, version_2=latest_version)
                or new_version == latest_version
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
                f"Unable to update 'latest' folder: the version {new_version} is older than 'latest' ({latest_version})"
            )
    if download_metadata:
        publish_key = PublishKey(product=draft_key.product, version=new_version)
        download_file(
            product_key=publish_key,
            filepath="build_metadata.json",
        )
        logger.info(f"Downloaded build_metadata.json from {publish_key.path}")


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
    max_files: int = typer.Option(
        s3.MAX_FILE_COUNT, "--max_files", help="Max number of files to upload"
    ),
):
    acl_literal = s3.string_as_acl(acl)
    if not output_path.exists():
        raise FileNotFoundError(f"Path {output_path} does not exist")
    build_name = build or BUILD_NAME
    if not build_name:
        raise ValueError(
            f"Build name supplied via CLI or the env var 'BUILD_NAME' cannot be '{build_name}'."
        )
    logger.info(
        f'Uploading {output_path} to {product}/build/{build_name} with ACL "{acl}"'
    )
    upload(
        output_path,
        BuildKey(product, build_name),
        acl=acl_literal,
        max_files=max_files,
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
):
    acl_literal = s3.string_as_acl(acl)
    logger.info(f'Promoting {product}/build/{build} to draft with ACL "{acl}"')
    promote_to_draft(
        build_key=BuildKey(product, build),
        draft_revision_summary=draft_summary,
        acl=acl_literal,
    )


@app.command("download_file")
def _cli_wrapper_download_file(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
        help="Name of data product (publishing folder in s3)",
    ),
    product_type: str = typer.Option(
        None,
        "-pt",
        "--product-type",
        help=f"Product type to download. Options are: 'build', 'draft', or 'publish'",
    ),
    version: str = typer.Option(None, "-v", "--version", help="Product version/build"),
    draft_revision: str = typer.Option(
        None,
        "-dr",
        "--draft-revision",
        help="If product-type is 'draft', must provide draft revision label (ex: 1-initial). Otherwise leave this blank",
    ),
    filepath: str = typer.Option(
        None, "-f", "--filepath", help="Filepath within s3 output folder"
    ),
    output_dir: Path = typer.Option(
        None, "-o", "--output-dir", help="Folder to download file to"
    ),
):
    if product_type == "draft":
        assert draft_revision is not None

    key: BuildKey | DraftKey | PublishKey
    match product_type:
        case "build":
            key = BuildKey(product=product, build=version)
        case "draft":
            key = DraftKey(product=product, version=version, revision=draft_revision)
        case "published":
            key = PublishKey(product=product, version=version)
        case _:
            raise ValueError(
                f"product-type should be 'build', 'draft', or 'published'. Instead got {product_type}"
            )
    download_file(key, filepath, output_dir)


if __name__ == "__main__":
    app()
