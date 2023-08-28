import os
from io import BytesIO
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


def _make_folder(s: str):
    return s if s[-1] == "/" else s + "/"


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


def list_objects(bucket: str, prefix: str):
    """Lists all objects with given prefix within bucket"""
    objects = []
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


def get_file(
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
        obj = client_.get_object(
            Bucket=bucket,
            Key=key,
            Filename=str(path),
            Callback=lambda bytes: progress.update(task, advance=bytes),
        )
        return obj["Body"]


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
    for obj in list_objects(bucket, prefix):
        key = obj["Key"].replace(prefix, "") if include_prefix_in_export else obj["Key"]
        if key and (key != prefix) and (key[-1] != "/"):
            key_directory = Path(key).parent
            (export_path / key_directory).mkdir(parents=True, exist_ok=True)
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
        relative_filepath = file.relative_to(local_folder_path)
        key = (
            upload_path / local_folder_path.name / relative_filepath
            if include_foldername
            else upload_path / relative_filepath
        )
        upload_file(bucket, file, str(key), acl=acl)


def copy_folder(
    bucket: str, source: str, target: str, acl: str, *, max_files: int = 20
):
    """Given bucket, prefix filter, and export path, download contents of folder from s3 recursively"""
    if source[-1] != "/":
        raise NotADirectoryError("prefix must be a folder path, ending with '/'")
    target = _make_folder(target)

    objects = list_objects(bucket, source)
    if len(objects) > max_files:
        raise Exception(
            f"{len(objects)} found in folder '{source}' which is greater than limit. Make sure target folder is correct, then supply 'max_files' arg"
        )
    for obj in objects:
        key = obj["Key"].replace(source, "")
        if key and (key[-1] != "/"):
            copy_file(bucket, obj["Key"], f"{target}{key}", acl)


def delete(bucket: str, path: str):
    """Deletes from s3 given path and bucket
    if slash is final character of path, assumed to be folder
    otherwise, assumed to be file"""
    client_ = client()
    if path[-1] == "/":
        for object in list_objects(bucket, path):
            client_.delete_object(Bucket=bucket, Key=object["Key"])
    client_.delete_object(Bucket=bucket, Key=path)


def get_filenames(bucket, prefix):
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


def get_file_as_stream(bucket, path):
    stream = BytesIO()
    client().download_fileobj(Bucket=bucket, Key=path, Fileobj=stream)
    stream.seek(0)
    return stream
