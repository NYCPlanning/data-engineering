from dataclasses import asdict
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

from dcpy.configuration import (
    PUBLISHING_BUCKET,
    CI,
    BUILD_NAME,
    DEV_FLAG,
    LOGGING_DB,
    LOGGING_SCHEMA,
    LOGGING_TABLE_NAME,
    PRODUCTS_TO_LOG,
    IGNORED_LOGGING_BUILDS,
)
from dcpy.connectors.registry import GenericConnector, VersionedConnector
from dcpy.models.connectors.edm.publishing import (
    ProductKey,
    PublishKey,
    DraftKey,
    BuildKey,
)
from dcpy.models.lifecycle.builds import BuildMetadata, EventLog, EventType
from dcpy.utils import s3, git, versions, metadata, postgres
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.recipes connector"
    )
    return PUBLISHING_BUCKET


def exists(key: ProductKey) -> bool:
    return s3.folder_exists(_bucket(), key.path)


def get_build_metadata(product_key: ProductKey) -> BuildMetadata:
    """Retrieve a product build metadata from s3."""
    key = f"{product_key.path}/build_metadata.json"
    bucket = _bucket()
    if not s3.object_exists(bucket, key):
        if not exists(product_key):
            raise FileNotFoundError(f"Product {product_key} does not exist.")
        else:
            raise FileNotFoundError(
                f"Build metadata not found for product {product_key}."
            )
    obj = s3.client().get_object(
        Bucket=bucket, Key=f"{product_key.path}/build_metadata.json"
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
    except FileNotFoundError:
        return None


def get_published_versions(product: str, exclude_latest: bool = True) -> list[str]:
    all_versions = s3.get_subfolders(_bucket(), f"{product}/publish/")
    output = (
        [v for v in all_versions if v != "latest"] if exclude_latest else all_versions
    )
    return sorted(output, reverse=True)


def get_draft_versions(product: str) -> list[str]:
    return sorted(s3.get_subfolders(_bucket(), f"{product}/draft/"), reverse=True)


def get_draft_version_revisions(product: str, version: str) -> list[str]:
    return sorted(
        s3.get_subfolders(_bucket(), f"{product}/draft/{version}/"), reverse=True
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
    return sorted(s3.get_subfolders(_bucket(), f"{product}/build/"), reverse=True)


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
    return s3.get_filenames(_bucket(), product_key.path)


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
        "date-created": datetime.now(pytz.timezone("America/New_York")).isoformat()
    }
    metadata["commit"] = git.commit_hash()
    if CI:
        metadata["run-url"] = git.action_url()
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
        _bucket(),
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
    bucket = _bucket()
    if s3_subpath is None:
        prefix = Path(publishing_folder)
    else:
        prefix = Path(publishing_folder) / s3_subpath
    key = prefix / version
    meta = generate_metadata()
    if output.is_dir():
        s3.upload_folder(
            bucket,
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
                bucket,
                str(key) + "/",
                str(prefix / "latest"),
                acl,
                max_files=max_files,
            )
    else:
        s3.upload_file(bucket, output, str(key), "public-read", metadata=meta)
        if latest:
            ## much faster than uploading again
            s3.copy_file(bucket, str(key), str(prefix / "latest" / output.name), acl)


def upload_build(
    output_path: Path,
    product: str,
    *,
    acl: s3.ACL,
    build: str | None = None,
    max_files: int = s3.MAX_FILE_COUNT,
) -> BuildKey:
    """
    Uploads a product build to an S3 bucket.

    This function handles uploading a local output folder to a specified
    location in an S3 bucket. The path, product, and build name must be
    provided, along with an optional ACL (Access Control List) to control
    file access in S3.

    Raises:
        FileNotFoundError: If the provided output_path does not exist.
        ValueError: If the build name is not provided and cannot be found in the environment variables.
    """
    if not output_path.exists():
        raise FileNotFoundError(f"Path {output_path} does not exist")
    build_name = build or BUILD_NAME
    if not build_name:
        raise ValueError(
            f"Build name supplied via CLI or the env var 'BUILD_NAME' cannot be '{build_name}'."
        )
    build_key = BuildKey(product, build_name)
    logger.info(f'Uploading {output_path} to {build_key.path} with ACL "{acl}"')
    upload(output_path, build_key, acl=acl, max_files=max_files)

    if (
        build_key.build not in IGNORED_LOGGING_BUILDS
        and build_key.product in PRODUCTS_TO_LOG
    ):
        version = get_version(build_key)
        run_details = metadata.get_run_details()
        event_metadata = EventLog(
            event=EventType.BUILD,
            product=build_key.product,
            version=version,
            path=build_key.path,
            old_path=None,
            timestamp=run_details.timestamp,
            runner_type=run_details.type,
            runner=run_details.runner_string,
        )
        log_event_in_db(event_metadata)

    return build_key


def promote_to_draft(
    build_key: BuildKey,
    acl: s3.ACL,
    keep_build: bool = True,
    max_files: int = s3.MAX_FILE_COUNT,
    draft_revision_summary: str = "",
    download_metadata: bool = False,
) -> DraftKey:
    bucket = _bucket()
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
    draft_key = DraftKey(build_key.product, version, draft_revision_label)
    source = build_key.path + "/"
    target = draft_key.path + "/"
    s3.copy_folder(bucket, source, target, acl, max_files=max_files)

    # upload updated metadata file
    s3.upload_file(
        bucket,
        build_metadata_path,
        f"{target}{build_metadata_path.name}",
        acl,
    )

    logger.info(
        f"Promoted {build_key.product} to drafts as {version}/{draft_revision_label}"
    )
    if not keep_build:
        s3.delete(bucket, source)

    if download_metadata:
        download_file(
            product_key=draft_key,
            filepath="build_metadata.json",
        )
        logger.info(f"Downloaded build_metadata.json from {draft_key.path}")

    run_details = metadata.get_run_details()
    event_metadata = EventLog(
        event=EventType.PROMOTE_TO_DRAFT,
        product=draft_key.product,
        version=draft_key.version,
        path=draft_key.path,
        old_path=build_key.path,
        timestamp=run_details.timestamp,
        runner_type=run_details.type,
        runner=run_details.runner_string,
    )
    log_event_in_db(event_metadata)

    return draft_key


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
) -> PublishKey:
    """Publishes a specific draft build of a data product
    By default, keeps draft output folder"""
    bucket = _bucket()
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

    publish_key = PublishKey(draft_key.product, new_version)
    source = draft_key.path + "/"
    target = publish_key.path + "/"
    s3.copy_folder(bucket, source, target, acl, max_files=max_files)

    # upload metadata if version was patched
    if build_metadata:
        s3.upload_file(
            bucket,
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
                bucket,
                source,
                f"{draft_key.product}/publish/latest/",
                acl,
                max_files=max_files,
            )
            if build_metadata:
                s3.upload_file(
                    bucket,
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

    run_details = metadata.get_run_details()
    event_metadata = EventLog(
        event=EventType.PUBLISH,
        product=publish_key.product,
        version=draft_key.version,  # this is release version, not patched
        path=publish_key.path,
        old_path=draft_key.path,
        timestamp=run_details.timestamp,
        runner_type=run_details.type,
        runner=run_details.runner_string,
    )
    log_event_in_db(event_metadata)

    return publish_key


def download_published_version(
    publish_key: PublishKey,
    output_dir: Path | None = None,
    *,
    include_prefix_in_export: bool = True,
) -> None:
    bucket = _bucket()
    output_dir = output_dir or Path(".")
    published_versions = get_published_versions(product=publish_key.product)
    assert publish_key.version in published_versions, (
        f"{publish_key} not found in S3 bucket '{bucket}'. Published versions are {published_versions}"
    )
    s3.download_folder(
        bucket,
        f"{publish_key.path}/",
        output_dir,
        include_prefix_in_export=include_prefix_in_export,
    )


def file_exists(product_key: ProductKey, filepath: str) -> bool:
    """Returns true if given file exists within outputs for given product key"""
    return s3.object_exists(bucket=_bucket(), key=f"{product_key.path}/{filepath}")


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


def download_file(
    product_key: ProductKey, filepath: str, output_dir: Path | None = None
) -> Path:
    output_dir = output_dir or Path(".")
    is_file_path = output_dir.suffix
    output_filepath = (
        output_dir / Path(filepath).name if not is_file_path else output_dir
    )
    logger.info(f"Downloading {product_key}, {filepath} -> {output_filepath}")
    s3.download_file(_bucket(), f"{product_key.path}/{filepath}", output_filepath)
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
    bucket = _bucket()
    if version is None:
        with s3.get_file(bucket, f"{source}version.txt") as f:
            version = str(f.read())
    old_metadata = s3.get_metadata(bucket, f"{source}{file_for_creation_date}")
    target = f"{product}/publish/{version}/"
    s3.copy_folder(
        bucket,
        source,
        target,
        acl,
        max_files=max_files,
        metadata={"date-created": old_metadata.last_modified.isoformat()},
    )
    return {"date-created": old_metadata.last_modified.isoformat()}


def get_data_directory_url(product_key: ProductKey) -> str:
    """Returns url of the data directory in Digital Ocean."""

    path = product_key.path
    if not path.endswith("/"):
        path += "/"
    endpoint = urlencode({"path": path})
    url = urljoin(f"https://cloud.digitalocean.com/spaces/{_bucket()}", "?" + endpoint)

    return url


def _gis_dataset_path(name: str, version: str) -> str:
    return f"datasets/{name}/{version}/{name}.zip"


def _assert_gis_dataset_exists(name: str, version: str):
    bucket = _bucket()
    version = version.upper()
    if not s3.object_exists(bucket, _gis_dataset_path(name, version)):
        raise FileNotFoundError(f"GIS dataset {name} has no version {version}")


def get_gis_dataset_versions(dataset_name: str, sort_desc: bool = True) -> list[str]:
    """
    Get all versions of GIS-published dataset in edm-publishing/datasets
    """
    gis_version_formats = [r"^\d{2}[A-Z]$", r"^\d{8}$"]
    subfolders = []
    matched_formats = set()
    for f in s3.get_subfolders(_bucket(), f"datasets/{dataset_name}"):
        for p in gis_version_formats:
            if re.match(p, f):
                subfolders.append(f)
                matched_formats.add(p)
    if subfolders:
        if len(matched_formats) > 1:
            raise ValueError(
                f"Multiple version formats found for gis dataset {dataset_name}. Cannot determine latest version"
            )
    return sorted(subfolders, reverse=sort_desc)


def get_latest_gis_dataset_version(dataset_name: str) -> str:
    """
    Get latest version of GIS-published dataset in edm-publishing/datasets
    assuming versions are sortable
    """
    versions = get_gis_dataset_versions(dataset_name)
    if not versions:
        raise FileNotFoundError(f"No versions found for GIS dataset {dataset_name}")
    version = versions[0]
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
    s3.download_file(_bucket(), _gis_dataset_path(dataset_name, version), file_path)
    return file_path


def log_event_in_db(event_details: EventLog) -> None:
    """
    Logs event metadata to a PostgreSQL database if the product is in the approved list
    of products and not in a development environment. Otherwise it skips logging.
    """

    if event_details.product not in PRODUCTS_TO_LOG:
        logger.warn(
            f"❗️ Product {event_details.product} not on the list of products to log in db. Skipping event metadata logging..."
        )
        return
    if DEV_FLAG:
        logger.info("DEV_FLAG env var found, skipping event metadata logging")
        return
    logger.info(
        f"Logging event '{event_details.event}' metadata for product {event_details.product} in db..."
    )
    pg_client = postgres.PostgresClient(database=LOGGING_DB, schema=LOGGING_SCHEMA)
    query = f"""
        INSERT INTO {LOGGING_SCHEMA}.{LOGGING_TABLE_NAME}
        (product, version, event, path, old_path, timestamp, runner_type, runner, custom_fields)
        VALUES
        (:product, :version, :event, :path, :old_path, :timestamp, :runner_type, :runner, :custom_fields)
        """
    pg_client.execute_query(
        query,
        product=event_details.product,
        version=event_details.version,
        event=event_details.event.value,
        path=event_details.path,
        old_path=event_details.old_path,
        timestamp=event_details.timestamp,
        runner_type=event_details.runner_type,
        runner=event_details.runner,
        custom_fields=json.dumps(event_details.custom_fields),
    )


class PublishedConnector(VersionedConnector):
    conn_type: str = "edm.publishing.published"

    def _pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        *,
        dataset: str | None = None,
        filepath: str,
        **kwargs,
    ) -> dict:
        pub_key = PublishKey(key, version)

        s3_path = dataset + "/" if dataset else ""

        pulled_path = download_file(
            pub_key,
            s3_path + filepath,
            output_dir=destination_path,
        )
        return {"path": pulled_path}

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        return self._pull(key, version, destination_path, **kwargs)

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise NotImplementedError()

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        return sorted(get_published_versions(key), reverse=sort_desc)

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self.list_versions(key)[0]

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)

    def data_local_sub_path(self, key: str, *, version: str, **kwargs) -> Path:  # type: ignore[override]
        return Path("edm") / "publishing" / "datasets" / key / version


class DraftsConnector(VersionedConnector):
    conn_type: str = "edm.publishing.drafts"

    def _pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        *,
        dataset: str | None = None,
        filepath: str,
        revision: str,
        **kwargs,
    ) -> dict:
        draft_key = DraftKey(key, version=version, revision=revision)

        path_prefix = dataset + "/" if dataset else ""
        file_path = f"{path_prefix}{filepath}"
        logger.info(f"Pulling Draft for {draft_key}, path={file_path}")
        pulled_path = download_file(draft_key, file_path, output_dir=destination_path)
        return {"path": pulled_path}

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        return self._pull(
            key, version=version, destination_path=destination_path, **kwargs
        )

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise NotImplementedError()

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        logger.info(f"Listing versions for {key}")
        versions = sorted(get_draft_versions(key), reverse=sort_desc)
        assert versions, (
            f"Product {key} should have versions, but none were found. This likely indicates a configuration problem."
        )
        return versions

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self.list_versions(key)[0]

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)

    def data_local_sub_path(
        self, key: str, *, version: str, revision: str, **kwargs
    ) -> Path:
        return Path("edm") / "publishing" / "datasets" / key / version / revision


class GisDatasetsConnector(VersionedConnector):
    conn_type: str = "edm.publishing.gis"

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        pulled_path = download_gis_dataset(
            dataset_name=key, version=version, target_folder=destination_path
        )
        return {"path": pulled_path}

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        raise PermissionError(
            "Currently, only GIS team pushes to edm-publishing/datasets"
        )

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        logger.info(f"Listing versions for {key}")
        return get_gis_dataset_versions(key, sort_desc=sort_desc)

    def get_latest_version(self, key: str, **kwargs) -> str:
        return get_latest_gis_dataset_version(key)

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return version in self.list_versions(key)


class BuildsConnector(GenericConnector):
    conn_type: str = "edm.publishing.builds"

    def push(self, key: str, **kwargs) -> dict:
        connector_args = kwargs["connector_args"]

        logger.info(
            f"Pushing build for product: {key}, with note: {connector_args['build_note']}"
        )
        result = upload_build(
            output_path=kwargs["build_path"],
            product=key,
            acl=s3.string_as_acl(connector_args["acl"]),
            build=connector_args["build_note"],
        )
        return asdict(result)

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        raise Exception("TODO")

    def data_local_sub_path(self, key: str, pull_conf) -> Path:
        assert pull_conf and "revision" in pull_conf
        return Path("edm") / "builds" / "datasets" / key / pull_conf["revision"]


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
        help="Product type to download. Options are: 'build', 'draft', or 'publish'",
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
