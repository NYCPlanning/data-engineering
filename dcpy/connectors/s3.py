from pathlib import Path
from pydantic import BaseModel

from dcpy.utils import s3
from dcpy.connectors.registry import StorageConnector


class Connector(BaseModel, StorageConnector):
    conn_type: str = "s3"
    bucket: str | None = None
    prefix: str | None = None

    def _bucket(self, conf: dict | None = None) -> str:
        if self.bucket:
            return self.bucket
        elif conf and "bucket" in conf:
            return conf["bucket"]
        else:
            raise Exception("No bucket defined")

    def push(self, key: str, push_conf: dict | None = {}) -> dict:
        push_conf = push_conf or {}
        if self.prefix:
            key = self.prefix + key
        acl = push_conf.get("acl", "private")
        bucket = self._bucket()

        if "filepath" in push_conf:
            s3.upload_file(
                bucket=bucket,
                path=push_conf["filepath"],
                key=key,
                acl=acl,
                metadata=push_conf.get("metadata"),
            )
        elif "file_obj" in push_conf:
            s3.upload_file_obj(
                bucket=bucket,
                file_obj=push_conf["file_obj"],
                key=key,
                acl=acl,
                metadata=push_conf.get("metadata"),
            )
        else:
            raise Exception(
                "Either filepath or file_obj must be specified to push to s3"
            )
        return {"bucket": bucket, "key": key}

    def pull(
        self,
        key: str,
        destination_path: Path,
        pull_conf: dict | None = None,
    ) -> dict:
        if self.prefix:
            key = self.prefix + key
        bucket = self._bucket()

        s3.download_file(bucket=bucket, key=key, path=destination_path)
        return {"path": destination_path}

    def get_current_version(self, key: str, **conf) -> str | None:
        return None
