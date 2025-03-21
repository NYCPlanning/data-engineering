import shutil
from pathlib import Path
from pydantic import BaseModel

from dcpy.connectors.registry import StorageConnector


class Connector(BaseModel, StorageConnector):
    conn_type: str = "drive"
    path: Path | None = None

    def _path(self, key: str, drive_path: Path | None = None) -> Path:
        if self.path:
            return self.path / key
        elif drive_path:
            return drive_path / key
        else:
            return Path(key)

    def push(self, key: str, push_conf: dict | None = {}) -> dict:
        assert push_conf and "filepath" in push_conf, (
            "filepath must be provided in push_conf"
        )
        path = self._path(key, push_conf)

        if path.exists() and not push_conf.get("overwrite"):
            raise Exception(f"'{path}' already exists.")
        shutil.copy(push_conf["filepath"], path / key)

        return {"path": path}

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        shutil.copy(self._path(key, **kwargs), destination_path)
        return {"path": destination_path}
