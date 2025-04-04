import shutil
from pathlib import Path
from pydantic import BaseModel

from dcpy.utils.logging import logger
from dcpy.connectors.registry import StorageConnector


class Connector(BaseModel, StorageConnector):
    conn_type: str = "drive"
    path: Path | None = None

    def _path(self, key: str, conf: dict | None = None) -> Path:
        if self.path:
            return self.path / key
        elif conf and "drive_path" in conf:
            return conf["drive_path"] / key
        else:
            return Path(key)

    def push(self, key: str, push_conf: dict | None = {}) -> dict:
        assert push_conf and "filepath" in push_conf, (
            "filepath must be provided in push_conf"
        )
        path = self._path(key, push_conf)

        if path.exists() and not push_conf.get("overwrite"):
            raise Exception(f"'{path}' already exists.")
        logger.info(f"Copying {push_conf['filepath']} to {path / key}")
        shutil.copy(push_conf["filepath"], path / key)

        return {"path": path}

    def pull(
        self,
        key: str,
        destination_path: Path,
        pull_conf: dict | None = None,
    ) -> dict:
        source_path = self._path(key, pull_conf)
        if source_path != destination_path:
            shutil.copy(source_path, destination_path)
        return {"path": destination_path}
