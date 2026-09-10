import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, ParamValidationError

if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import ObjectIdentifierTypeDef
else:
    ObjectIdentifierTypeDef = object

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from . import aws_s3_bucket

# TODO - remove this when data-library moved to dcpy s3 functionality
ACL = Literal[
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-full-control",
    "bucket-owner-read",
    "private",
    "public-read",
    "public-read-write",
]


class S3:
    def __init__(
        self,
    ):
        config = Config(read_timeout=120)
        aws_s3_endpoint = os.environ.get("AWS_S3_ENDPOINT")
        aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=aws_s3_endpoint,
            config=config,
        )
        self.bucket = aws_s3_bucket
        if self.bucket not in [
            b["Name"] for b in self.client.list_buckets()["Buckets"]
        ]:
            self.client.create_bucket(Bucket=self.bucket)

    def upload_file(
        self, name: str, version: str, path: str, acl: ACL = "public-read"
    ) -> None:
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        """
        suffix = Path(path).suffix
        key = f"{name}/{version}/{name}{suffix}"
        self.put(path, key, acl)

    def put(
        self,
        path: str,
        key: str,
        acl: ACL = "public-read",
        metadata: dict | None = None,
    ) -> None:
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

            def update_progress(bytes):
                progress.update(task, advance=bytes)

            extraArgs: dict[str, Any] = {"ACL": acl}
            if metadata:
                extraArgs["Metadata"] = metadata

            try:
                self.client.upload_file(
                    path,
                    self.bucket,
                    key,
                    ExtraArgs=extraArgs,
                    Callback=update_progress,
                )
            except ClientError as e:
                logging.error(e)

    def exists(self, key: str):
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def ls(self, prefix: str, detail: bool = False) -> list:
        response = self.client.list_objects(Bucket=self.bucket, Prefix=prefix)
        if "Contents" in response.keys():
            contents = response["Contents"]
            if detail:
                return contents
            else:
                return [content["Key"] for content in contents]
        else:
            return []

    # https://s3fs.readthedocs.io/en/latest/api.html?highlight=listdir#s3fs.core.S3FileSystem.info

    def info(self, key: str) -> dict[str, Any]:
        """
        Get header info for a given file
        """
        response = self.client.head_object(Bucket=self.bucket, Key=key)
        # Set custom metadata keys to lowercase for compatibility with both
        # DigitalOcean and minio standards
        if "Metadata" in response:
            meta_lower = {k.lower(): v for k, v in response["Metadata"].items()}
            response.update({"Metadata": meta_lower})
            return cast(dict[str, Any], response)
        else:
            return {}

    def cp(
        self,
        source_key: str,
        dest_key: str,
        acl: ACL = "public-read",
        metadata: dict = {},
        info: bool = False,
    ) -> dict | None:
        """
        Copy a file to a new path within the same bucket

        Parameters
        ----------
        key: path within the bucket of the file to copy
        dest_ket: new path for the copy
        acl: acl for newly created file
        metadata: dictionary to save as custom s3 metadata
        """
        try:
            self.client.copy_object(
                Bucket=self.bucket,
                Key=dest_key,
                CopySource={"Bucket": self.bucket, "Key": source_key},
                ACL=acl,
                Metadata=metadata,
            )
            if info:
                return self.info(key=dest_key)
            else:
                return None
        except ParamValidationError as e:
            raise ValueError(f"Copy {source_key} -> {dest_key} failed: {e}") from e

    def rm(self, *keys) -> dict:
        """
        Removes a files within the bucket
        sample usage:
        s3.rm('path/to/file')
        s3.rm('file1', 'file2', 'file3')
        s3.rm(*['file1', 'file2', 'file3'])
        """
        objects: list[ObjectIdentifierTypeDef] = [{"Key": k} for k in keys]
        response = self.client.delete_objects(
            Bucket=self.bucket, Delete={"Objects": objects, "Quiet": False}
        )
        return cast(dict[str, Any], response)

    def mv(
        self,
        source_key: str,
        dest_key: str,
        acl: ACL = "public-read",
        metadata: dict = {},
        info: bool = False,
    ) -> dict | None:
        """
        Move a file to a new path within the same bucket.
        Calls cp then rm

        Parameters
        ----------
        source_key: path within the bucket of the file to move
        dest_ket: new path for the copy
        acl: acl for newly created file
        metadata: dictionary to save as custom s3 metadata
        info: if true, get info for file in its new location
        """

        self.cp(source_key=source_key, dest_key=dest_key, acl=acl, metadata=metadata)
        self.rm(source_key)
        if info:
            return self.info(key=dest_key)
        else:
            return None
