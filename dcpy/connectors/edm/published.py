import json
import pandas as pd
from pathlib import Path
import yaml

from dcpy.configuration import (
    PUBLISHING_BUCKET,
)
from dcpy.connectors.registry import VersionedConnector
from dcpy.models.connectors.edm.publishing import (
    ProductKey,
    PublishKey,
    DraftKey,
)
from dcpy.models.lifecycle.builds import BuildMetadata
from dcpy.utils import s3, versions
from dcpy.utils.logging import logger


def _bucket() -> str:
    assert PUBLISHING_BUCKET, (
        "'PUBLISHING_BUCKET' must be defined to use edm.recipes connector"
    )
    return PUBLISHING_BUCKET


_TEMP_PUBLISHING_FILE_SUFFIXES = {
    ".zip",
    ".parquet",
    ".csv",
    ".pdf",
    ".xlsx",
    ".json",
    ".text",
}


def read_csv(product_key: ProductKey, filepath: str, **kwargs) -> pd.DataFrame:
    """Reads csv into pandas dataframe from edm-publishing
    Works for zipped or standard csvs

    Keyword arguments:
    product_key -- a key to find a specific instance of a data product
    filepath -- the filepath of the desired csv in the output folder
    """
    return _read_data_helper(f"{product_key.path}/{filepath}", pd.read_csv, **kwargs)


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


def download_file(
    product_key: ProductKey, filepath: str, output_dir: Path | None = None
) -> Path:
    output_dir = output_dir or Path(".")
    is_file_path = output_dir.suffix in _TEMP_PUBLISHING_FILE_SUFFIXES
    output_filepath = (
        output_dir / Path(filepath).name if not is_file_path else output_dir
    )
    logger.info(f"Downloading {product_key}, {filepath} -> {output_filepath}")
    s3.download_file(_bucket(), f"{product_key.path}/{filepath}", output_filepath)
    return output_filepath


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
