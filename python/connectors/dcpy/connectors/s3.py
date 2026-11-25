from pathlib import Path

from dcpy import configuration
from dcpy.connectors.registry import StorageConnector
from dcpy.utils import s3
from dcpy.utils.logging import logger


class S3Connector(StorageConnector):
    conn_type: str = "s3"
    bucket: str | None = None
    prefix: str | None = None

    def _bucket(self, bucket: str | None = None) -> str:
        if self.bucket and bucket:
            raise Exception(
                f"Connector '{self.conn_type}' is configured for bucket '{self.bucket}', cannot provide bucket '{bucket}' as kwarg"
            )
        elif self.bucket:
            return self.bucket
        elif bucket:
            return bucket
        else:
            raise Exception("No bucket defined")

    def _apply_prefix(self, key: str) -> str:
        return (self.prefix or "") + key

    def _locate(self, key: str, bucket: str | None = None) -> tuple[str, str]:
        """Bucket and key to use, after the prefix and the INBOX_ROOT_FOLDER remap."""
        return configuration.inbox_location(
            self._bucket(bucket), self._apply_prefix(key)
        )

    def exists(self, key: str, bucket: str | None = None) -> bool:
        return s3.object_exists(*self._locate(key, bucket))

    def get_subfolders(self, prefix: str, bucket: str | None = None) -> list[str]:
        return s3.get_subfolders(*self._locate(prefix, bucket))

    def _push(
        self,
        key: str,
        *,
        filepath: Path,
        bucket: str | None = None,
        acl: s3.ACL = "private",
        metadata: dict | None = None,
        **kwargs,
    ) -> dict:
        bucket, key = self._locate(key, bucket)

        logger.info(f"Pushing {filepath} to {bucket}/{key}")
        s3.upload_file(
            bucket=bucket,
            path=filepath,
            key=key,
            acl=acl,
            metadata=metadata,
        )
        return {"bucket": bucket, "key": key}

    def pull(
        self,
        key: str,
        destination_path: Path,
        *,
        bucket: str | None = None,
        **kwargs,
    ) -> dict:
        bucket, key = self._locate(key, bucket)
        logger.info(f"Pulling {bucket}/{key}")
        filepath = s3.download_file(bucket=bucket, key=key, path=destination_path)
        return {"path": filepath}

    def push(self, key, **kwargs) -> dict:
        return self._push(key, **kwargs)
