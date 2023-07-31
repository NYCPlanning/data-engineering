import os
from pathlib import Path
from typing import Optional
import boto3
from botocore.client import Config

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)


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
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    endpoint_url=os.environ["AWS_S3_ENDPOINT"],
):
    """Returns a client for S3."""
    config = Config(read_timeout=120)
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        config=config,
    )


def download_file(
    bucket: str,
    key: str,
    path: Path,
):
    """Downloads a file from S3"""
    with _progress() as progress:
        client_ = client()
        size = client_.head_object(Bucket=bucket, Key=key)["ContentLength"]
        task = progress.add_task(
            f"[green]Downloading [bold]{Path(key).name}[/bold]", total=size
        )
        client_.download_file(
            Bucket=bucket,
            Key=key,
            Filename=str(path),
            Callback=lambda bytes: progress.update(task, advance=bytes),
        )


def upload_file(
    bucket: str,
    path: Path,
    key: str,
    acl: str,
    *,
    metadata: Optional[dict] = None,
) -> dict:
    """Uploads a single file to AWS S3.
    adapted from data library
    https://github.com/NYCPlanning/db-data-library/blob/main/library/s3.py#L37"""
    with _progress() as progress:
        size = os.stat(path).st_size
        task = progress.add_task(
            f"[green]Uploading [bold]{path.name}[/bold]", total=size
        )
        extra_args = {"ACL": acl}
        if metadata:
            extra_args["Metadata"] = metadata
        response = client().upload_file(
            path,
            bucket,
            key,
            ExtraArgs=extra_args,
            Callback=lambda bytes: progress.update(task, advance=bytes),
        )
    return response


def copy_file(
    bucket: str, source_key: str, target_key: str, acl: str, target_bucket: str = None
):
    if target_bucket is None:
        target_bucket = bucket
    """Copies a file from from one location in S3 to another"""
    with _progress() as progress:
        client_ = client()
        size = client_.head_object(Bucket=bucket, Key=source_key)["ContentLength"]
        task = progress.add_task(
            f"[green]Copying [bold]{Path(source_key).name}[/bold]", total=size
        )
        client_.copy(
            CopySource={"Bucket": bucket, "Key": source_key},
            Bucket=target_bucket,
            Key=target_key,
            ExtraArgs={"ACL": acl},
            Callback=lambda bytes: progress.update(task, advance=bytes),
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

    client_ = client()
    resp = client_.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if not "Contents" in resp:
        raise NotADirectoryError(f"Folder {prefix} not found in bucket {bucket}")
    for obj in resp["Contents"]:
        key = obj["Key"].replace(prefix, "") if include_prefix_in_export else obj["Key"]
        if key and (key != prefix) and (key[-1] != "/"):
            key_directory = Path(key).parent
            if not (export_path / key_directory).exists():
                os.makedirs(export_path / key_directory)
            download_file(bucket, obj["Key"], export_path / key)


def upload_folder(
    bucket: str,
    local_folder_path: Path,
    upload_path: str,
    acl: str,
    *,
    max_files: int = 20,
    include_foldername: bool = True,
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
        key = (
            upload_path / file
            if include_foldername
            else upload_path / Path(*file.parts[2:])
        )
        upload_file(bucket, file, str(key), acl=acl)


def copy_folder(
    bucket: str, source: str, target: str, acl: str, *, max_files: int = 20
):
    """Given bucket, prefix filter, and export path, download contents of folder from s3 recursively"""
    if source[-1] != "/":
        raise NotADirectoryError("prefix must be a folder path, ending with '/'")
    if target[-1] != "/":
        target += "/"

    client_ = client()
    resp = client_.list_objects_v2(Bucket=bucket, Prefix=source)
    if len(resp) > max_files:
        raise Exception(
            f"{len(resp)} found in folder '{source}' which is greater than limit. Make sure target folder is correct, then supply 'max_files' arg"
        )
    if not "Contents" in resp:
        raise NotADirectoryError(f"Folder {source} not found in bucket {bucket}")
    for obj in resp["Contents"]:
        key = obj["Key"].replace(source, "")
        if key and (key[-1] != "/"):
            copy_file(bucket, obj["Key"], f"{target}{key}", acl)
