import os
from pathlib import Path
import boto3
from botocore.client import Config

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)


def client(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    endpoint_url=os.environ["AWS_S3_ENDPOINT"],
):
    """Returns a client for AWS S3."""
    config = Config(read_timeout=120)
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        config=config,
    )


def upload_file(
    bucket: str,
    path: str,
    key: str,
    acl: str = "public-read",
    metadata: dict = None,
) -> dict:
    """Uploads a single file to AWS S3.
    adapted from data library
    https://github.com/NYCPlanning/db-data-library/blob/main/library/s3.py#L37"""
    with Progress(
        SpinnerColumn(spinner_name="earth"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        transient=True,
    ) as progress:
        size = os.stat(path).st_size
        task = progress.add_task(
            f"[green]Uploading [bold]{os.path.basename(path)}[/bold]", total=size
        )

        response = client().upload_file(
            path,
            bucket,
            key,
            ExtraArgs={"ACL": acl, "Metadata": metadata},
            Callback=lambda complete: progress.update(task, completed=complete),
        )
    return response


def download_folder(
    bucket: str, prefix: str, export_path: str, include_prefix_in_export: bool = False
) -> None:
    """Given bucket, prefix filter, and export path, download contents of folder from s3 recursively"""
    export_path = Path(export_path)
    if prefix[-1] != "/":
        raise NotADirectoryError("prefix must be a folder path, ending with '/'")

    client_ = client()
    resp = client_.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if not "Contents" in resp:
        raise NotADirectoryError(f"Folder {prefix} not found in bucket {bucket}")
    for obj in resp["Contents"]:
        key = obj["Key"].replace(prefix, "")
        if key and (key[-1] != "/"):
            key_directory = os.path.dirname(key)
            if include_prefix_in_export:
                export_path = export_path / prefix
            if not (export_path / key_directory).exists():
                os.makedirs(export_path / key_directory)
            client_.download_file(bucket, obj["Key"], export_path / key)


def upload_folder(
    bucket: str,
    local_folder_path: str,
    upload_path: str,
    acl: str = "public-read",
    metadata: dict = None,
    include_foldername: bool = True,
) -> None:
    """Given bucket, local folder path, and upload path, uploads contents of folder to s3 recursively"""
    local_folder_path = Path(local_folder_path)
    if not local_folder_path.exists() or (not local_folder_path.is_dir()):
        raise NotADirectoryError(f"'{local_folder_path}' is not a folder.")
    for path_object in local_folder_path.rglob("*"):
        if path_object.is_file():
            if not include_foldername:
                key = upload_path / Path(*path_object.parts[2:])
            else:
                key = Path(upload_path) / path_object
            upload_file(bucket, path_object, str(key), acl=acl, metadata=metadata)
