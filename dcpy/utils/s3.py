import os
from io import BytesIO
from pathlib import Path
import typer
from typing import Any, Literal, TYPE_CHECKING, cast, get_args
import boto3
from botocore.response import StreamingBody
from botocore.client import Config

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

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


def string_as_acl(s: str) -> ACL:
    if not s in get_args(ACL):
        raise ValueError(f"String '{s}' is not a valid literal 'ACL'")
    return cast(ACL, s)


def _make_folder(s: str):
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


def client(
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    endpoint_url: str | None = None,
) -> S3Client:
    """Returns a client for S3."""
    config = Config(read_timeout=120)
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        config=config,
    )


def list_objects(bucket: str, prefix: str) -> list:
    """Lists all objects with given prefix within bucket"""
    objects: list = []
    prefix = _make_folder(prefix)
    try:
        paginator = client().get_paginator("list_objects_v2")
        for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = objects + result.get("Contents", [])
    except Exception as exc:
        print(f"get_objects(bucket={bucket}, prefix={prefix}) failed")
        raise exc
    return objects


def download_file(
    bucket: str,
    key: str,
    path: Path,
) -> None:
    """Downloads a file from S3"""
    with _progress() as progress:
        size = get_metadata(bucket, key)["content-length"]
        task = progress.add_task(
            f"[green]Downloading [bold]{Path(key).name}[/bold]", total=size
        )
        client().download_file(
            Bucket=bucket,
            Key=key,
            Filename=str(path),
            Callback=lambda bytes: progress.update(task, advance=bytes),
        )


def get_metadata(bucket: str, key: str) -> dict[str, Any]:
    """Gets custom metadata as well as three standard s3 fields"""
    response = client().head_object(Bucket=bucket, Key=key)
    metadata: dict[str, Any] = response["Metadata"]
    metadata["last-modified"] = response["LastModified"]
    metadata["content-length"] = response["ContentLength"]
    metadata["content-type"] = response["ContentType"]
    return metadata


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
        extra_args: dict[Any, Any] = {"ACL": acl}
        if metadata:
            extra_args["Metadata"] = metadata
        client().upload_file(
            str(path),
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
    all_metadata = client_.head_object(Bucket=bucket, Key=source_key)
    meta = all_metadata.get("Metadata", {})
    if metadata is not None:
        meta.update(metadata)
    client_.copy_object(
        CopySource={"Bucket": bucket, "Key": source_key},
        Bucket=target_bucket,
        Key=target_key,
        ACL=acl,
        Metadata=meta,
        MetadataDirective="REPLACE",
    )


def download_folder(
    bucket: str,
    prefix: str,
    export_path: Path,
    *,
    include_prefix_in_export: bool = False,
) -> None:
    """Given bucket, prefix filter, and export path, download contents of folder from s3 recursively"""
    if prefix[-1] != "/":
        raise NotADirectoryError("prefix must be a folder path, ending with '/'")
    for obj in list_objects(bucket, prefix):
        key = obj["Key"].replace(prefix, "") if include_prefix_in_export else obj["Key"]
        if key and (key != prefix) and (key[-1] != "/"):
            key_directory = Path(key).parent
            (export_path / key_directory).mkdir(parents=True, exist_ok=True)
            download_file(bucket, obj["Key"], export_path / key)


def upload_folder(
    bucket: str,
    local_folder_path: Path,
    upload_path: Path,
    acl: ACL,
    *,
    max_files: int = MAX_FILE_COUNT,
    contents_only: bool = False,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Given bucket, local folder path, and upload path, uploads contents of folder to s3 recursively"""
    if not local_folder_path.exists() or (not local_folder_path.is_dir()):
        raise NotADirectoryError(f"'{local_folder_path}' is not a folder.")
    files = [object for object in local_folder_path.rglob("*") if object.is_file()]
    if len(files) > max_files:
        raise Exception(
            f"{len(files)} found in folder '{local_folder_path}' which is greater than limit. Make sure target folder is correct, then supply 'max_files' arg"
        )
    for file in files:
        relative_filepath = file.relative_to(local_folder_path)
        key = (
            upload_path / relative_filepath
            if contents_only
            else upload_path / local_folder_path.name / relative_filepath
        )
        upload_file(bucket, file, str(key), acl=acl, metadata=metadata)


def copy_folder(
    bucket: str,
    source: str,
    target: str,
    acl: ACL,
    *,
    max_files: int = MAX_FILE_COUNT,
    metadata: dict[str, Any] | None = None,
    target_bucket: str | None = None,
) -> None:
    """Given bucket, prefix filter, and export path, download contents of folder from s3 recursively"""
    if source[-1] != "/":
        raise NotADirectoryError("prefix must be a folder path, ending with '/'")
    target = _make_folder(target)

    objects = list_objects(bucket, source)
    if max_files and (len(objects) > max_files):
        raise Exception(
            f"{len(objects)} found in folder '{source}' which is greater than limit. Make sure target folder is correct, then supply 'max_files' arg"
        )
    for obj in objects:
        key = obj["Key"].replace(source, "")
        if key and (key[-1] != "/"):
            copy_file(
                bucket,
                obj["Key"],
                f"{target}{key}",
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
            client_.delete_object(Bucket=bucket, Key=object["Key"])
    client_.delete_object(Bucket=bucket, Key=path)


def get_filenames(bucket: str, prefix: str) -> set[str]:
    return {
        obj["Key"].split("/")[-1]
        for obj in list_objects(bucket, prefix)
        if obj["Key"].split("/")[-1] != ""
    }


def get_subfolders(bucket: str, prefix: str, index=1):
    prefix = _make_folder(prefix)
    prefix_path = Path(prefix)
    subfolders = set()
    try:
        paginator = client().get_paginator("list_objects_v2")
        for result in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for obj in result.get("CommonPrefixes", []):
                path = Path(obj["Prefix"])
                if len(path.relative_to(prefix_path).parts) == index:
                    subfolders.add(path.name)
    except Exception as exc:
        print(f"get_subfolders(bucket={bucket}, prefix={prefix}) failed")
        raise exc
    return list(subfolders)


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
):
    acl_literal = string_as_acl(acl)
    upload_folder(
        bucket,
        local_path,
        s3_path,
        acl=acl_literal,
        max_files=max_files,
        contents_only=contents_only,
    )


if __name__ == "__main__":
    app()
