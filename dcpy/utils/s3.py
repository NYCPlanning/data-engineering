import boto3
from botocore.response import StreamingBody
from botocore.client import Config
from botocore.exceptions import ClientError
from datetime import datetime
from io import BytesIO
import os
from pathlib import Path
from pyarrow import fs
import pytz
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
from tempfile import TemporaryDirectory
import typer
from typing import Any, Literal, TYPE_CHECKING, cast, get_args
from pydantic import BaseModel

from dcpy.utils import git
from dcpy.utils.logging import logger

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

ACL = Literal[
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-full-control",
    "bucket-owner-read",
    "private",
    "public-read",
    "public-read-write",
]
MAX_FILE_COUNT = 50


class Metadata(BaseModel):
    last_modified: datetime
    content_length: int
    content_type: str
    custom: dict[str, Any]


def generate_metadata() -> dict[str, str]:
    metadata = {
        "date-created": datetime.now(pytz.timezone("America/New_York")).isoformat()
    }
    try:
        metadata["commit"] = git.commit_hash()
    except Exception:
        pass
    if os.environ.get("CI"):
        metadata["run-url"] = git.action_url()
    return metadata


def string_as_acl(s: str) -> ACL:
    if s not in get_args(ACL):
        raise ValueError(f"String '{s}' is not a valid literal 'ACL'")
    return cast(ACL, s)


def _folderize(s: str):
    """
    Converts a non-empty string into a directory. Note, empty string
    shouldn't be converted as it refers to a root directory.
    """
    if s != "":
        return s if s[-1] == "/" else s + "/"
    else:
        return s


def _progress():
    return Progress(
        SpinnerColumn(spinner_name="earth"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        transient=False,
    )


def client() -> S3Client:
    """Returns a client for S3."""
    aws_s3_endpoint = os.environ.get("AWS_S3_ENDPOINT")
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    config = Config(read_timeout=120)
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=aws_s3_endpoint,
        config=config,
    )


def list_buckets() -> list[str]:
    """Get list of buckets for the defined connection variables"""
    return [b["Name"] for b in client().list_buckets()["Buckets"]]


def get_bucket_region(bucket: str) -> str:
    """
    Gets region (with subregion suffix) of specific bucket
    DISCLAIMER - this might only work for Digital Ocean Spaces.
        Documentation on format of these responses is not the clearest,
        nor is there evident documentation on format of these id strings
    """
    bucket_id = client().head_bucket(Bucket=bucket)["ResponseMetadata"]["RequestId"]
    return bucket_id.split("-")[-1]


def list_objects(bucket: str, prefix: str) -> list[dict]:
    """Lists all objects with given prefix within bucket"""
    objects: list[dict] = []
    try:
        paginator = client().get_paginator("list_objects_v2")
        for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = objects + cast(list[dict], result.get("Contents", []))
    except Exception as exc:
        logger.info(f"get_objects(bucket={bucket}, prefix={prefix}) failed. {str(exc)}")
        raise exc
    return objects


def object_exists(bucket: str, key: str) -> bool:
    """Returns true if an object with given bucket and key exists"""
    try:
        client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def folder_exists(bucket: str, prefix: str) -> bool:
    prefix = _folderize(prefix)
    resp = client().list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1)
    return "Contents" in resp


def get_custom_metadata(bucket: str, key: str) -> dict:
    return client().head_object(Bucket=bucket, Key=key)["Metadata"]


def get_metadata(bucket: str, key: str) -> Metadata:
    """Gets custom metadata as well as three standard s3 fields"""
    response = client().head_object(Bucket=bucket, Key=key)
    return Metadata(
        last_modified=response["LastModified"],
        content_length=response["ContentLength"],
        content_type=response["ContentType"],
        custom=response["Metadata"],
    )


def get_file(
    bucket: str,
    key: str,
) -> StreamingBody:
    """Downloads a file from S3"""
    obj = client().get_object(
        Bucket=bucket,
        Key=key,
    )
    return obj["Body"]


def download_file(
    bucket: str,
    key: str,
    path: Path,
) -> None:
    """Downloads a file from S3"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with _progress() as progress:
        size = get_metadata(bucket, key).content_length
        task = progress.add_task(
            f"[green]Downloading [bold]{Path(key).name}[/bold]", total=size
        )
        client().download_file(
            Bucket=bucket,
            Key=key,
            Filename=str(path),
            Callback=lambda bytes: progress.update(task, advance=bytes),
        )


def upload_file(
    bucket: str,
    path: Path,
    key: str,
    acl: ACL,
    *,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Uploads a single file to AWS S3.
    adapted from data library
    https://github.com/NYCPlanning/db-data-library/blob/main/library/s3.py#L37"""
    with _progress() as progress:
        size = os.stat(path).st_size
        task = progress.add_task(
            f"[green]Uploading [bold]{path.name}[/bold]", total=size
        )
        standard_metadata = generate_metadata()
        metadata = metadata or {}
        metadata.update(standard_metadata)
        extra_args: dict[Any, Any] = {"ACL": acl, "Metadata": metadata}
        client().upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs=extra_args,
            Callback=lambda bytes: progress.update(task, advance=bytes),
        )


def upload_file_obj(
    file_obj: BytesIO,
    bucket: str,
    key: str,
    acl: ACL,
    *,
    metadata: dict[str, Any] | None = None,
) -> None:
    """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_fileobj.html"""
    with _progress() as progress:
        size = file_obj.getbuffer().nbytes
        task = progress.add_task(f"[green]Uploading [bold]{key}[/bold]", total=size)
        standard_metadata = generate_metadata()
        metadata = metadata or {}
        metadata.update(standard_metadata)
        extra_args: dict[Any, Any] = {"ACL": acl, "Metadata": metadata}
        client().upload_fileobj(
            file_obj,
            bucket,
            key,
            ExtraArgs=extra_args,
            Callback=lambda bytes: progress.update(task, advance=bytes),
        )


def copy_file(
    bucket: str,
    source_key: str,
    target_key: str,
    acl: ACL,
    *,
    target_bucket: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Copies a file from from one location in S3 to another"""
    if target_bucket is None:
        target_bucket = bucket
    client_ = client()
    existing_metadata = get_metadata(bucket, source_key).custom
    if metadata is not None:
        existing_metadata.update(metadata)
    client_.copy_object(
        CopySource={"Bucket": bucket, "Key": source_key},
        Bucket=target_bucket,
        Key=target_key,
        ACL=acl,
        Metadata=existing_metadata,
        MetadataDirective="REPLACE",
    )


def download_folder(
    bucket: str,
    prefix: str,
    export_path: Path,
    *,
    include_prefix_in_export: bool = True,
) -> list[dict]:
    """
    Download contents of folder from s3 recursively.
    Returns list of objects downloaded.
    """
    prefix = _folderize(prefix)

    objs = list_objects(bucket, prefix)
    if not objs:
        raise Exception(
            f"Empty listing returned for {bucket}/{prefix}. This might indicate that the path doesn't exist."
        )
    for obj in objs:
        key = obj["Key"] if include_prefix_in_export else obj["Key"].replace(prefix, "")
        if key and (key != prefix) and (key[-1] != "/"):
            key_directory = Path(key).parent
            (export_path / key_directory).mkdir(parents=True, exist_ok=True)
            download_file(bucket, obj["Key"], export_path / key)
    return objs


def upload_folder(
    bucket: str,
    local_folder_path: Path,
    upload_path: Path,
    acl: ACL,
    *,
    max_files: int = MAX_FILE_COUNT,
    contents_only: bool = False,
    metadata: dict[str, Any] | None = None,
    keep_existing: bool = False,
) -> None:
    """Given bucket, local folder path, and upload path, uploads contents of folder to s3 recursively"""
    if not local_folder_path.exists() or (not local_folder_path.is_dir()):
        raise NotADirectoryError(f"'{local_folder_path}' is not a folder.")
    files = [object for object in local_folder_path.rglob("*") if object.is_file()]
    if len(files) > max_files:
        raise Exception(
            f"{len(files)} found in folder '{local_folder_path}' which is greater than limit. Make sure target folder is correct, then supply 'max_files' arg"
        )

    if not keep_existing:
        logger.info(
            f"Deleting any existing files in {upload_path} from bucket {bucket}"
        )
        delete(bucket, _folderize(str(upload_path)))

    logger.info(f"Uploading {local_folder_path} to {upload_path} in bucket {bucket}")
    for file in files:
        relative_filepath = file.relative_to(local_folder_path)
        key = (
            upload_path / relative_filepath
            if contents_only
            else upload_path / local_folder_path.name / relative_filepath
        )
        upload_file(bucket, file, str(key), acl=acl, metadata=metadata)


def copy_folder_via_download(
    source_bucket: str, target_bucket: str, source_path: str, target_path: str, acl: ACL
) -> None:
    """If cross-bucket copying is not possible, this utility downloads and then uploads a folder"""
    with TemporaryDirectory() as _dir:
        dir = Path(_dir)
        logger.info(f"Copying {source_bucket}/{source_path} to temporary directory")
        download_folder(source_bucket, prefix=source_path, export_path=dir)
        logger.info(f"Copying temporary directory to {target_bucket}/{source_path}")
        upload_folder(
            target_bucket,
            dir / source_path,
            Path(target_path),
            acl,
            contents_only=True,
        )


def copy_folder(
    bucket: str,
    source_path: str,
    target_path: str,
    acl: ACL,
    *,
    max_files: int = MAX_FILE_COUNT,
    metadata: dict[str, Any] | None = None,
    target_bucket: str | None = None,
    keep_existing: bool = False,
) -> None:
    """Given bucket, prefix filter, and export path, download contents of folder from s3 recursively"""
    source_path = _folderize(source_path)
    target_path = _folderize(target_path)

    objects = list_objects(bucket, source_path)
    if max_files and (len(objects) > max_files):
        raise Exception(
            f"{len(objects)} found in folder '{source_path}' which is greater than limit. Make sure target folder is correct, then supply 'max_files' arg"
        )
    target_bucket_ = target_bucket or bucket
    if not keep_existing:
        logger.info(
            f"Deleting any existing files in {target_bucket_}/{target_path} from bucket "
        )
        delete(target_bucket_, target_path)

    if target_bucket and get_bucket_region(bucket) != get_bucket_region(target_bucket):
        if get_bucket_region(bucket) != get_bucket_region(target_bucket):
            copy_folder_via_download(
                bucket, target_bucket, source_path, target_path, acl
            )
            return
    else:
        target_bucket = target_bucket or bucket

        logger.info(f"Copying {bucket}/{source_path} to {target_bucket}/{target_path}")
        for obj in objects:
            key = obj["Key"].replace(source_path, "")
            print(key)
            print(target_path)
            if key and (key[-1] != "/"):
                copy_file(
                    bucket,
                    obj["Key"],
                    f"{target_path}{key}",
                    acl,
                    metadata=metadata,
                    target_bucket=target_bucket,
                )


def delete(bucket: str, path: str) -> None:
    """Deletes from s3 given path and bucket
    if slash is final character of path, assumed to be folder
    otherwise, assumed to be file"""
    client_ = client()
    if path[-1] == "/":
        for object in list_objects(bucket, path):
            print(object["Key"])
            client_.delete_object(Bucket=bucket, Key=object["Key"])
    client_.delete_object(Bucket=bucket, Key=path)


def get_suffixes(bucket: str, prefix: str) -> set[str]:
    """Gets all suffixes of objects in bucket given a prefix"""
    return {
        obj["Key"].removeprefix(prefix)
        for obj in list_objects(bucket, prefix)
        if obj["Key"].removeprefix(prefix) != ""
    }


def get_filenames(bucket: str, prefix: str) -> set[str]:
    """Given folder path prefix, gets filenames of all objects within folder"""
    prefix = _folderize(prefix)
    return get_suffixes(bucket, prefix)


def get_subfolders(bucket: str, prefix: str, index=1) -> list[str]:
    """
    List subfolders in an S3 bucket at a specific depth.

    Parameters:
    bucket (str): The name of the S3 bucket.
    prefix (str): The prefix (directory path) to search within the bucket.
    index (int): The depth level of subfolders to list (default is 1 for top-level subfolders).

    Returns:
    list[str]: A list of subfolder names at the specified depth.

    Examples:
    >>> # Given the following objects in the S3 bucket:
    >>> # - 'root/folder1/file1.txt'
    >>> # - 'root/folder1/folder2/file2.txt'
    >>> # - 'root/folder3/'
    >>>
    >>> get_subfolders('my_bucket', 'root/', 1)
    ['folder1', 'folder3']
    >>>
    >>> get_subfolders('my_bucket', 'root/', 2)
    ['folder1/folder2']
    """
    prefix = _folderize(prefix)
    prefix_path = Path(prefix)
    subfolders: set[str] = set()

    try:
        all_objects = list_objects(bucket=bucket, prefix=prefix)
        for obj in all_objects:
            obj_path = Path(obj["Key"])

            # Determine the directory path (if object is a file, get parent directory)
            dir_path = obj_path if obj["Key"].endswith("/") else obj_path.parent

            # Get relative parts of the directory path after the prefix
            relative_parts = dir_path.relative_to(prefix_path).parts

            # Add the directory path up to the specified folder depth to the subfolders set
            if len(relative_parts) >= index:
                partial_path = Path(*relative_parts[:index])
                subfolders.add(str(partial_path))

    except Exception as exc:
        print(f"get_subfolders(bucket={bucket}, prefix={prefix}, index={index}) failed")
        raise exc

    return sorted(list(subfolders))


def get_file_as_stream(bucket: str, path: str) -> BytesIO:
    stream = BytesIO()
    client().download_fileobj(Bucket=bucket, Key=path, Fileobj=stream)
    stream.seek(0)
    return stream


def get_file_as_text(bucket: str, path: str) -> str:
    resp = client().get_object(Bucket=bucket, Key=path).get("Body")
    if resp:
        return resp.read().decode()
    else:
        raise Exception(f"No body found for file '{path}' in bucket '{bucket}'.")


def pyarrow_fs() -> fs.S3FileSystem:
    return fs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_override=os.environ.get("AWS_S3_ENDPOINT"),
    )


app = typer.Typer(add_completion=False)


# ? deprecate this CLI in favor of connectors.edm modules?
@app.command("upload_folder")
def _cli_wrapper_upload_folder(
    bucket: str = typer.Option(None, "-b", "--bucket", help="S3 bucket"),
    local_path: Path = typer.Option(
        None, "--folder-path", help="Path to local output folder"
    ),
    s3_path: Path = typer.Option(None, "--s3-path", help="Path to s3 output folder"),
    acl: str = typer.Option(None, "-a", "--acl", help="Access level of file in s3"),
    max_files: int = typer.Option(
        20, "--max-files", help="Maximum number of files to upload"
    ),
    contents_only: bool = typer.Option(
        False,
        "--contents-only",
        help="If true, uploads local folder into target folder. If false, uploads contents of local folder instead",
    ),
    keep_existing: bool = typer.Option(
        False,
        "--keep-existing",
        help="If true, does not delete target folder in s3 prior to loading",
    ),
):
    upload_folder(
        bucket,
        local_path,
        s3_path,
        acl=string_as_acl(acl),
        max_files=max_files,
        contents_only=contents_only,
        keep_existing=keep_existing,
    )


@app.command("download_file")
def _cli_wrapper_download_file(bucket: str, s3_path: str, local_path: Path):
    logger.info(f"Downloading file at {bucket}/{s3_path} to {local_path}")
    download_file(bucket, s3_path, local_path)
